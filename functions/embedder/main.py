"""
Cloud Function: Embedding Processor
- Converts text chunks into vector embeddings
- Uses checkpointing to resume if interrupted
"""

import os
import json
import logging
import time
import numpy as np
from google.cloud import storage
from sentence_transformers import SentenceTransformer

# GCS Buckets
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "your-output-bucket")

# Load Sentence Transformer Model
model = SentenceTransformer("all-MiniLM-L6-v2")

# Initialize Storage Client
storage_client = storage.Client()

def load_checkpoint(folder_prefix):
    """
    Loads embedding checkpoint to resume if needed.
    """
    metadata_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"structured_data/{folder_prefix}/embeddings/embeddings_metadata.json"
    )
    if metadata_blob.exists():
        return json.loads(metadata_blob.download_as_text())
    return {"processed_chunks": []}  # Default if no checkpoint exists

def save_checkpoint(folder_prefix, checkpoint_data):
    """
    Saves embedding progress.
    """
    metadata_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"structured_data/{folder_prefix}/embeddings/embeddings_metadata.json"
    )
    metadata_blob.upload_from_string(json.dumps(checkpoint_data))

def log_failed_embedding(folder_prefix, chunk_filename, error_msg):
    """
    Logs failed embeddings so they can be retried later.
    """
    errors_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"structured_data/{folder_prefix}/embeddings/embeddings_errors.json"
    )
    if errors_blob.exists():
        errors_data = json.loads(errors_blob.download_as_text())
    else:
        errors_data = {"failed_chunks": []}

    errors_data["failed_chunks"].append(
        {"chunk": chunk_filename, "error": error_msg, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")}
    )

    errors_blob.upload_from_string(json.dumps(errors_data))

def process_embedding(event, context):
    """
    Cloud Function Trigger:
    - Reads text chunks
    - Converts them into vector embeddings
    - Saves embeddings and checkpoint progress
    """
    file_path = event["name"]
    logging.info(f"New chunk uploaded for embedding: {file_path}")

    folder_prefix = file_path.split("/")[1]  # Extract folder name

    # Load checkpoint
    checkpoint = load_checkpoint(folder_prefix)
    processed_chunks = set(checkpoint["processed_chunks"])

    # Load previous errors
    errors_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"structured_data/{folder_prefix}/embeddings/embeddings_errors.json"
    )
    if errors_blob.exists():
        failed_chunks = json.loads(errors_blob.download_as_text())["failed_chunks"]
    else:
        failed_chunks = []

    # Get all chunk files
    chunk_blobs = list(storage_client.bucket(OUTPUT_BUCKET).list_blobs(prefix=f"structured_data/{folder_prefix}/chunks/"))

    for chunk_blob in chunk_blobs:
        chunk_filename = chunk_blob.name.split("/")[-1]
        embedding_filename = chunk_filename.replace(".json", ".npy")

        # Skip already processed embeddings
        if embedding_filename in processed_chunks:
            continue

        try:
            # Read chunk text
            chunk_data = json.loads(chunk_blob.download_as_text())
            chunk_text = chunk_data.get("text", "")

            # Skip empty text
            if not chunk_text.strip():
                logging.warning(f"Skipping empty chunk: {chunk_filename}")
                continue

            # Generate embedding
            embedding_vector = model.encode(chunk_text)

            # Save embedding
            embedding_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
                f"structured_data/{folder_prefix}/embeddings/{embedding_filename}"
            )
            np.save(embedding_blob.open("wb"), embedding_vector)

            # Update checkpoint
            processed_chunks.add(embedding_filename)
            save_checkpoint(folder_prefix, {"processed_chunks": list(processed_chunks)})

            # Remove from error log if it was a retry
            failed_chunks = [c for c in failed_chunks if c["chunk"] != chunk_filename]
            errors_blob.upload_from_string(json.dumps({"failed_chunks": failed_chunks}))

        except Exception as e:
            logging.error(f"Embedding failed for {chunk_filename}: {e}")
            log_failed_embedding(folder_prefix, chunk_filename, str(e))

    logging.info(f"Embedding process complete for {folder_prefix}")
