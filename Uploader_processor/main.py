"""
FastAPI Chunk Processor Service

This API allows users to:
1. Upload documents to GCS (SOURCE_BUCKET).
2. If file > 50MB, split into multiple chunks. Otherwise keep single chunk.
3. Generate a manifest.json under gs://SOURCE_BUCKET/<folder_uuid>/manifest.json

Access the API docs at:
- Swagger UI: /docs
- Redoc UI: /redoc
"""

import os
import uuid
import json
import logging
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from google.cloud import storage
from chunker import split_file_into_chunks
from gcs_utils import upload_to_gcs, create_manifest
from config import SOURCE_BUCKET, CHUNK_SIZE_MB

app = FastAPI(
    title="Chunk Processor API",
    description="API to upload large files to GCS, split them if needed, and generate a manifest file.",
    version="1.0"
)

class UploadResponse(BaseModel):
    """Response model for file uploads"""
    message: str
    folder_uuid: str
    manifest_uri: str
    chunk_uris: List[str]

@app.get("/", summary="Health Check", tags=["Health"])
async def health_check():
    """Checks if the API is running."""
    return {"status": "running"}

@app.post("/upload", response_model=UploadResponse, summary="Upload and Process File", tags=["File Processing"])
async def upload_document(file: UploadFile = File(...)):
    """
    Uploads a file to GCS, splits it into CHUNK_SIZE_MB chunks if needed, and creates a manifest.

    Returns JSON with:
      - 'folder_uuid': Unique folder for the uploaded doc
      - 'chunk_uris': GCS paths of chunked files
      - 'manifest_uri': GCS path of the manifest
    """
    try:
        filename = file.filename
        local_path = f"/tmp/{filename}"
        with open(local_path, "wb") as buffer:
            buffer.write(await file.read())

        # Generate a unique folder name in GCS, e.g. abcd-1234
        folder_uuid = str(uuid.uuid4())[:8]  # shortened for clarity

        # Check file size for chunking
        file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
        if file_size_mb <= CHUNK_SIZE_MB:
            # Single chunk
            chunk_paths = [local_path]
        else:
            # Split into multiple chunks
            chunk_paths = split_file_into_chunks(local_path, CHUNK_SIZE_MB)

        # Upload chunk(s) to GCS
        # They will be named chunk_000, chunk_001, etc.
        chunk_uris = upload_to_gcs(chunk_paths, SOURCE_BUCKET, folder_prefix=f"{folder_uuid}/")

        # Create a manifest file in the same folder
        manifest_uri = create_manifest(
            original_filename=filename,
            chunk_uris=chunk_uris,
            folder_uuid=folder_uuid,
            bucket_name=SOURCE_BUCKET
        )

        # Cleanup local temp files
        for path in chunk_paths:
            if path != local_path:  # avoid double remove if single chunk
                try:
                    os.remove(path)
                except:
                    pass

        # Also remove the original local file if splitted
        try:
            os.remove(local_path)
        except:
            pass

        return {
            "message": "File uploaded and processed successfully",
            "folder_uuid": folder_uuid,
            "chunk_uris": chunk_uris,
            "manifest_uri": manifest_uri
        }
    except Exception as e:
        logging.error(f"Error processing file {file.filename}: {e}")
        raise HTTPException(status_code=500, detail=f"File processing error: {e}")