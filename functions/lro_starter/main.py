"""
Cloud Function: LRO Starter for Document AI

This function is triggered when a new chunk is uploaded to the `chunks/` directory in GCS.
It ensures all chunks of a document exist before triggering Document AI batch processing.

Functionality:
1. **Triggered on new chunk uploads**.
2. **Is the manifest present?**
3. **Checks if all chunks exist by comparing against the manifest file**.
4. **Do the file names match the manifest?**.
5. **If all chunks are present, starts an LRO for Document AI processing**.
6. **Uses exponential backoff for retrying failure**.
7. **Stores LRO metadata in GCS for tracking**.

Metadata Checkpointing
    Keeps track of LRO status in GCS, so if a failure occurs, 
    it resumes from last known state instead of restarting.

Exponential Backoff	
    Gradually increases retry time for failures, preventing excessive retries and giving 
    the system time to recover.

Requirements:
- Cloud Function must have permissions for:
  - `storage.objects.get`
  - `storage.objects.list`
  - `storage.objects.update`
  - `documentai.processors.batchProcess`
"""

import os
import json
import time
import logging
from google.cloud import storage, documentai_v1beta3 as documentai
from google.api_core.retry import Retry

# GCS Buckets (Chunks + Manifest stored here)
CHUNK_BUCKET = os.getenv("CHUNK_BUCKET", "your-chunk-bucket")
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "your-output-bucket")

# Document AI Processor
PROCESSOR_ID = os.getenv("DOCUMENT_AI_PROCESSOR", "your-processor-id")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
REGION = os.getenv("REGION", "us")

# Initialize Clients
storage_client = storage.Client()
documentai_client = documentai.DocumentUnderstandingServiceClient()


def get_folder_prefix(file_path: str) -> str:
    """
    Extracts the unique folder prefix from the file path.

    Example:
    - `chunks/abcd-1234/manifests_sample.json` → `abcd-1234/`
    - `chunks/abcd-1234/sample_chunk_1.pdf` → `abcd-1234/`

    Args:
        file_path (str): The GCS file path.

    Returns:
        str: Folder prefix (UUID folder).
    """
    return file_path.split("/")[1]  # Extracts `abcd-1234`


def check_all_chunks_present(folder_prefix: str, file_prefix: str) -> bool:
    """
    Checks if all chunks for a document are uploaded by verifying with the manifest file.

    Manifest is expected at: `gs://CHUNK_BUCKET/{folder_prefix}/manifests_{file_prefix}.json`
    Chunks are expected at: `gs://CHUNK_BUCKET/{folder_prefix}/{file_prefix}_chunk_*.pdf`

    Args:
        folder_prefix (str): The unique folder for this document batch.
        file_prefix (str): The document's base filename.

    Returns:
        bool: True if all chunks + manifest exist, False otherwise.
    """

    manifest_blob_path = f"{folder_prefix}/manifests_{file_prefix}.json"
    manifest_blob = storage_client.bucket(CHUNK_BUCKET).blob(manifest_blob_path)

    if not manifest_blob.exists():
        logging.warning(f"Manifest file not found: {manifest_blob_path}. Skipping processing.")
        return False

    # Read manifest file
    manifest_data = json.loads(manifest_blob.download_as_text())
    expected_chunks = manifest_data.get("num_chunks", 0)
    expected_chunk_files = set(manifest_data.get("chunk_files", []))  # Expected filenames

    # Count actual uploaded chunks
    chunk_blobs = storage_client.bucket(CHUNK_BUCKET).list_blobs(prefix=f"{folder_prefix}/{file_prefix}_chunk_")
    actual_chunk_files = {blob.name.split("/")[-1] for blob in chunk_blobs}

    logging.info(f"Checking chunks for {file_prefix}: Expected {expected_chunks}, Found {len(actual_chunk_files)}")

    if len(actual_chunk_files) == expected_chunks and expected_chunk_files == actual_chunk_files:
        logging.info(f"All required chunks are present for {file_prefix}.")
        return True
    else:
        logging.info(f"Waiting for all chunks. Found: {actual_chunk_files}, Expected: {expected_chunk_files}")
        return False


@Retry(initial=1.0, maximum=60.0, multiplier=2.0, deadline=600.0)
def start_lro(folder_prefix: str, file_prefix: str):
    """
    Starts a Long-Running Operation (LRO) for Document AI batch processing.

    Args:
        folder_prefix (str): The unique UUID folder for this batch.
        file_prefix (str): The original document filename prefix.

    Raises:
        Exception: If LRO fails to start.
    """
    input_gcs_uri = f"gs://{CHUNK_BUCKET}/{folder_prefix}/{file_prefix}_chunk_*"
    output_gcs_uri = f"gs://{OUTPUT_BUCKET}/processed/{folder_prefix}/"

    request = {
        "name": f"projects/{PROJECT_ID}/locations/{REGION}/processors/{PROCESSOR_ID}:batchProcess",
        "inputDocuments": {
            "gcsDocuments": {
                "documents": [{"gcs_uri": input_gcs_uri, "mime_type": "application/pdf"}]
            }
        },
        "outputConfig": {"gcsDestination": {"uri": output_gcs_uri}, "pagesPerShard": 10}
    }

    operation = documentai_client.batch_process_documents(request)
    lro_id = operation.operation.name

    # Store in GCS metadata
    metadata_blob = storage_client.bucket(OUTPUT_BUCKET).blob(f"metadata/{folder_prefix}/{file_prefix}.json")
    metadata_blob.upload_from_string(json.dumps({"lro_id": lro_id, "status": "IN_PROGRESS"}))

    logging.info(f"Started LRO for {file_prefix} in {folder_prefix}, LRO ID: {lro_id}")


def lro_starter(event, context):
    """
     Cloud Function Entry Point.

    - Triggered when **any chunk OR manifest file** is uploaded to `CHUNK_BUCKET`
    - Waits until **all chunks + manifest** exist before triggering Document AI processing.

    Args:
        event (dict): Event payload from Cloud Storage.
        context (google.cloud.functions.Context): Metadata about the event.

    Returns:
        None
    """
    file_path = event["name"]
    logging.info(f"New file uploaded: {file_path}")

    # Extract UUID folder and file prefix
    folder_prefix = get_folder_prefix(file_path)

    if file_path.startswith(f"{folder_prefix}/manifests_"):
        file_prefix = file_path.split("/")[-1].replace("manifests_", "").replace(".json", "")
    elif file_path.startswith(f"{folder_prefix}/") and "_chunk_" in file_path:
        file_prefix = file_path.split("/")[-1].rsplit("_chunk_", 1)[0]
    else:
        logging.info("Ignoring file (not a chunk or manifest).")
        return

    if check_all_chunks_present(folder_prefix, file_prefix):
        logging.info(f"All chunks for {file_prefix} in {folder_prefix} are present. Starting LRO...")
        start_lro(folder_prefix, file_prefix)
    else:
        logging.info(f"Chunks for {file_prefix} in {folder_prefix} are not yet complete. Waiting for more uploads.")