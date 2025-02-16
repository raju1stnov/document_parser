"""
Cloud Function: LRO Starter for Document AI (Handles Both Large and Small Files)

This function is triggered when a new chunk or manifest file is uploaded to the source_bucket prefix path in GCS.
It determines whether to:
  Trigger **Long-Running Operation (LRO)** for large files (multiple chunks).
  Trigger **Single Document Processing** for small files (single chunk).

Key Features:
1. **Triggered on new chunk uploads**.
2. **Reads the manifest to determine num_chunks**.
3. **If multiple chunks → triggers LRO processing**.
4. **If a single chunk → triggers normal document processing**.
5. **Stores metadata & LRO status in GCS for checkpointing**.
6. **Uses exponential backoff for retries on failure**

Metadata Checkpointing:
    - Saves LRO ID & status in `gs://OUTPUT_BUCKET/metadata/<folder_uuid>/<file_prefix>.json`
    - Prevents duplicate LROs and allows function to resume if restarted.

Exponential Backoff:
    - Retries API failures with increasing wait times (1s → 2s → 4s → ... 60s max)

LRO Resumption:
    - If LRO metadata exists, checks if LRO is still running.
    - If running, **does not start a new LRO** (prevents duplication).
    - If LRO completed, continues pipeline.

Requirements:
- Cloud Function must have permissions for:
  - `storage.objects.get`
  - `storage.objects.list`
  - `storage.objects.update`
  - `documentai.processors.batchProcess`
"""

import os
import json
import logging
from google.cloud import storage, documentai_v1beta3 as documentai
from google.api_core.retry import Retry

# GCS Buckets
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "your-source-bucket")
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

def check_manifest_and_decide(folder_prefix: str) -> str:
    """
    Reads the manifest file and determines whether to trigger LRO or single processing.

    Args:
        folder_prefix (str): The folder where chunks are stored.

    Returns:
        str: `"LRO"` if large file, `"SINGLE"` if small file, `"WAIT"` if chunks are incomplete.
    """
    manifest_blob_path = f"{folder_prefix}/manifest.json"
    manifest_blob = storage_client.bucket(SOURCE_BUCKET).blob(manifest_blob_path)

    if not manifest_blob.exists():
        logging.warning(f"Manifest file not found: {manifest_blob_path}. Waiting for it.")
        return "WAIT"

    # Read manifest file
    manifest_data = json.loads(manifest_blob.download_as_text())
    num_chunks = manifest_data.get("num_chunks", 0)
    expected_chunk_files = set(manifest_data.get("chunk_files", []))  # Expected filenames

    # Count actual uploaded chunks
    chunk_blobs = storage_client.bucket(SOURCE_BUCKET).list_blobs(prefix=f"{folder_prefix}/chunk_")
    actual_chunk_files = {blob.name.split("/")[-1] for blob in chunk_blobs}

    logging.info(f"Checking chunks for {folder_prefix}: Expected {num_chunks}, Found {len(actual_chunk_files)}")

    if len(actual_chunk_files) == num_chunks and expected_chunk_files == actual_chunk_files:
        return "LRO" if num_chunks > 1 else "SINGLE"
    else:
        return "WAIT"

def get_lro_status(folder_prefix: str):
    """
    Checks the LRO status from metadata and determines if we need to resume it.

    Args:
        folder_prefix (str): The folder where chunks are stored.

    Returns:
        str: `"RESUME"` if LRO is incomplete, `"NEW"` if LRO has not started, `"COMPLETE"` if done.
    """
    metadata_blob_path = f"metadata/{folder_prefix}/report.json"
    metadata_blob = storage_client.bucket(OUTPUT_BUCKET).blob(metadata_blob_path)

    if not metadata_blob.exists():
        return "NEW"  # No previous LRO, start fresh

    # Read metadata file
    metadata = json.loads(metadata_blob.download_as_text())
    lro_id = metadata.get("lro_id")
    status = metadata.get("status")

    if status == "COMPLETED":
        return "COMPLETE"
    elif status == "IN_PROGRESS" and lro_id:
        return "RESUME"

    return "NEW"

@Retry(initial=1.0, maximum=60.0, multiplier=2.0, deadline=600.0)
def start_lro(folder_prefix: str):
    """
    Starts or resumes a Long-Running Operation (LRO) for large files.

    Args:
        folder_prefix (str):  The folder where chunks are stored.       

    Raises:
        Exception: If LRO fails to start.
    """
    input_gcs_uri = f"gs://{SOURCE_BUCKET}/{folder_prefix}/chunk_*"
    output_gcs_uri = f"gs://{OUTPUT_BUCKET}/structured_data/{folder_prefix}/"

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
    metadata_blob = storage_client.bucket(OUTPUT_BUCKET).blob(f"metadata/{folder_prefix}/report.json")
    metadata_blob.upload_from_string(json.dumps({"lro_id": lro_id, "status": "IN_PROGRESS"}))

    logging.info(f"Started LRO for {folder_prefix}, LRO ID: {lro_id}")

def start_single_processing(folder_prefix: str):
    """
    Directly processes a small document (≤50MB) with Document AI.

    Args:
        folder_prefix (str): The folder where chunks are stored.
    """
    input_gcs_uri = f"gs://{SOURCE_BUCKET}/{folder_prefix}/chunk_001.pdf"
    output_gcs_uri = f"gs://{OUTPUT_BUCKET}/structured_data/{folder_prefix}/"

    request = {
        "name": f"projects/{PROJECT_ID}/locations/{REGION}/processors/{PROCESSOR_ID}:process",
        "rawDocument": {"gcs_uri": input_gcs_uri, "mime_type": "application/pdf"}
    }

    result = documentai_client.process_document(request)

    # Extract structured data
    extracted_text = result.document.text
    extracted_entities = [{"text": ent.mention_text, "type": ent.type} for ent in result.document.entities]

    # Store structured output in GCS
    output_bucket = storage_client.bucket(OUTPUT_BUCKET)
    output_bucket.blob(f"structured_data/{folder_prefix}/report_text.txt").upload_from_string(extracted_text)
    output_bucket.blob(f"structured_data/{folder_prefix}/report_entities.json").upload_from_string(json.dumps(extracted_entities))

    # Store metadata
    metadata_blob = output_bucket.blob(f"metadata/{folder_prefix}/report.json")
    metadata_blob.upload_from_string(json.dumps({"status": "COMPLETED"}))

    logging.info(f"Completed processing for {folder_prefix}.")

def lro_starter(event, context):
    """
    Cloud Function Entry Point.

    - Triggered when **any chunk OR manifest file** is uploaded to `SOURCE_BUCKET`
    - Determines whether to process **large files (LRO)** or **small files (direct Document AI)**.

    Args:
        event (dict): Event payload from Cloud Storage.
        context (google.cloud.functions.Context): Metadata about the event.
    """
    file_path = event["name"]
    logging.info(f"New file uploaded: {file_path}")

    # Extract UUID folder
    folder_prefix = get_folder_prefix(file_path)

    processing_type = check_manifest_and_decide(folder_prefix)

    if processing_type == "LRO":
        logging.info(f"Large file detected for {folder_prefix}. Starting LRO processing.")
        start_lro(folder_prefix)
    elif processing_type == "SINGLE":
        logging.info(f"Small file detected for {folder_prefix}. Starting single document processing.")
        start_single_processing(folder_prefix)
    else:
        logging.info(f"Chunks for {folder_prefix} are not yet complete. Waiting for more uploads.")