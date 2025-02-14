"""
FastAPI Chunk Processor Service

This API allows users to:
1. Upload documents to GCS.
2. Split large documents into 20MB chunks.
3. Generate a manifest file for tracking.

Access the API docs at:
- Swagger UI: /docs
- Redoc UI: /redoc
"""

import os
import json
import logging
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from google.cloud import storage
from chunker import split_file_into_chunks
from gcs_utils import upload_to_gcs, generate_manifest
from config import UPLOAD_BUCKET, CHUNK_BUCKET, MANIFEST_BUCKET

# FastAPI App with Metadata
app = FastAPI(
    title="Chunk Processor API",
    description="API to upload large files to GCS, split them into chunks, and generate a manifest file.",
    version="1.0"
)

class UploadResponse(BaseModel):
    """Response model for file uploads"""
    message: str
    original_file_uri: str
    chunks: List[str]
    manifest_uri: str

@app.get("/", summary="Health Check", tags=["Health"])
async def health_check():
    """Checks if the API is running."""
    return {"status": "running"}

@app.post("/upload", response_model=UploadResponse, summary="Upload and Process File", tags=["File Processing"])
async def upload_file(file: UploadFile = File(...)):
    """
    Uploads a file to GCS, splits it into 20MB chunks if necessary, and creates a manifest.

    - **file**: The document to be uploaded.
    - **Returns**: JSON with original file location, chunked file URIs, and manifest location.
    """
    try:
        # Save the file locally
        filename = file.filename
        local_path = f"/tmp/{filename}"
        with open(local_path, "wb") as buffer:
            buffer.write(await file.read())

        # Upload the original file to GCS
        gcs_uri = upload_to_gcs(local_path, UPLOAD_BUCKET, f"originals/{filename}")

        # Split file into chunks
        chunk_paths = split_file_into_chunks(local_path)

        # Upload chunks to GCS
        chunk_uris = upload_to_gcs(chunk_paths, CHUNK_BUCKET, prefix="chunks/")

        # Generate a manifest file
        manifest_uri = generate_manifest(
            original_filename=filename,
            chunk_uris=chunk_uris,
            upload_bucket=MANIFEST_BUCKET
        )

        # Cleanup local temp files
        os.remove(local_path)
        for chunk in chunk_paths:
            os.remove(chunk)

        return {
            "message": "File uploaded and processed successfully",
            "original_file_uri": gcs_uri,
            "chunks": chunk_uris,
            "manifest_uri": manifest_uri
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")
