"""
Cloud Function: Chunk Processor (Fault-Tolerant)
- Splits structured document text into smaller chunks.
- Uses checkpointing to resume from failures.
- Logs errors for failed chunks and retries later.
"""

import os
import json
import logging
import time
from google.cloud import storage
from google.api_core.retry import Retry
from chunker import chunk_text

# GCS Buckets
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "your-output-bucket")

# Initialize Storage Client
storage_client = storage.Client()


def load_checkpoint(folder_prefix):
    """
    Loads chunking checkpoint from GCS to resume if needed.
    If checkpoint does not exist, return an empty progress state.
    """
    metadata_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"structured_data/{folder_prefix}/chunks/chunks_metadata.json"
    )
    if metadata_blob.exists():
        return json.loads(metadata_blob.download_as_text())
    return {"processed_chunks": []}


def save_checkpoint(folder_prefix, checkpoint_data):
    """
    Saves the current progress of processed chunks to GCS.
    """
    metadata_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"structured_data/{folder_prefix}/chunks/chunks_metadata.json"
    )
    metadata_blob.upload_from_string(json.dumps(checkpoint_data))


def load_failed_chunks(folder_prefix):
    """
    Loads previously failed chunk records for retry.
    """
    error_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"structured_data/{folder_prefix}/chunks/chunks_errors.json"
    )
    if error_blob.exists():
        return json.loads(error_blob.download_as_text())
    return {"failed_chunks": []}


def save_failed_chunk(folder_prefix, chunk_filename, error_message):
    """
    Saves failed chunk logs to a GCS file so we can retry them later.
    """
    error_log = load_failed_chunks(folder_prefix)
    error_log["failed_chunks"].append({"chunk": chunk_filename, "error": error_message})

    error_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"structured_data/{folder_prefix}/chunks/chunks_errors.json"
    )
    error_blob.upload_from_string(json.dumps(error_log))


@Retry(initial=2.0, maximum=60.0, multiplier=2.0, deadline=600.0)
def process_chunking(event, context):
    """
    Cloud Function Trigger:
    - Reads structured document text from GCS.
    - Chunks the text.
    - Saves chunks & uses checkpointing for fault tolerance.
    - Logs failed chunks & retries later.
    """
    file_path = event["name"]
    logging.info(f"New structured data uploaded: {file_path}")

    folder_prefix = file_path.split("/")[1]  # Extract folder name

    try:
        # Load full text
        text_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
            f"structured_data/{folder_prefix}/report_text.txt"
        )
        document_text = text_blob.download_as_text()
    except Exception as e:
        logging.error(f"Failed to read report_text.txt: {e}")
        return

    # Load previous progress
    checkpoint = load_checkpoint(folder_prefix)
    processed_chunks = set(checkpoint["processed_chunks"])

    # Load failed chunks to retry
    failed_chunks = load_failed_chunks(folder_prefix)
    retry_chunks = {entry["chunk"] for entry in failed_chunks["failed_chunks"]}

    # Chunk the text
    chunks = chunk_text(document_text)

    # Save chunks with error handling
    for idx, chunk in enumerate(chunks):
        chunk_filename = f"chunk_{idx+1:03}.json"

        # Skip already processed chunks
        if chunk_filename in processed_chunks:
            continue

        try:
            chunk_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
                f"structured_data/{folder_prefix}/chunks/{chunk_filename}"
            )
            chunk_blob.upload_from_string(json.dumps({"text": chunk}))

            # Update checkpoint
            processed_chunks.add(chunk_filename)
            save_checkpoint(folder_prefix, {"processed_chunks": list(processed_chunks)})

            # If this chunk was in the retry list, remove it from error logs
            if chunk_filename in retry_chunks:
                failed_chunks["failed_chunks"] = [
                    entry for entry in failed_chunks["failed_chunks"] if entry["chunk"] != chunk_filename
                ]
                save_failed_chunk(folder_prefix, failed_chunks)  # Update error log

        except Exception as e:
            logging.error(f"Error processing chunk {chunk_filename}: {e}")
            save_failed_chunk(folder_prefix, chunk_filename, str(e))  # Log failure for retry later

    logging.info(f"Chunking complete for {folder_prefix}")
