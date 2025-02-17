"""
main.py

Demonstration of a fault-tolerant Document AI LRO script handling multiple file types (PDF, DOC, DOCX, XLS, XLSX).

Features:
1. Reads a 'manifest.json' from a GCS source folder (e.g. upload-<random>).
2. Starts or resumes a Document AI LRO using @Retry for transient failures.
3. Stores parse_manifest.json in GCS to track LRO status (IN_PROGRESS, SUCCESS, or FAILED).
4. If the script is re-run, it reads parse_manifest.json and decides whether to resume or skip.
5. Uses correct snake_case fields for v1beta3:
   - 'input_documents' -> 'gcs_documents' -> 'documents'
   - 'document_output_config' -> 'gcs_output_config'
6. guess_mime_type() to handle doc, docx, xls, xlsx, pdf.

Folder structure (example):
gs://bucket_name/myname/source_path/
    upload-164654/
        my_test_pdf.pdf
        my_test_doc.docx
        my_test_excel.xls
        manifest.json

gs://bucket_name/myname/output_path/structured_data/
    upload-164654/
        unstructured_data.txt
        parse_manifest.json
"""

import os
import json
import time
import logging
from datetime import datetime
from google.cloud import storage, documentai_v1beta3 as documentai
from google.api_core.retry import Retry

# ------------- Configuration -------------
SOURCE_BUCKET = "bucket_name"  # e.g. your bucket
SOURCE_PREFIX = "myname/source_path/"  # e.g. "myname/source_path/"

OUTPUT_BUCKET = "bucket_name"  # same or different bucket
OUTPUT_PREFIX = "myname/output_path/structured_data/"  # e.g. "myname/output_path/structured_data/"

PROJECT_ID = "your-gcp-project-id"
REGION = "us"  # e.g. 'us' or 'us-central1'
PROCESSOR_ID = "6780"  # Document AI processor

# Initialize Clients
storage_client = storage.Client()
documentai_client = documentai.DocumentProcessorServiceClient()

def guess_mime_type(filename: str) -> str:
    """
    Returns an appropriate MIME type based on file extension.
    If unrecognized, returns 'application/octet-stream'.
    """
    fn = filename.lower()
    if fn.endswith(".pdf"):
        return "application/pdf"
    elif fn.endswith(".doc"):
        return "application/msword"
    elif fn.endswith(".docx"):
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    elif fn.endswith(".xls"):
        return "application/vnd.ms-excel"
    elif fn.endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        return "application/octet-stream"

def read_manifest(upload_folder: str) -> dict:
    """
    Reads the manifest.json from GCS that lists the files to be processed.
    """
    manifest_blob = storage_client.bucket(SOURCE_BUCKET).blob(
        f"{SOURCE_PREFIX}{upload_folder}/manifest.json"
    )
    if not manifest_blob.exists():
        raise FileNotFoundError(f"manifest.json not found under {upload_folder}.")

    data = json.loads(manifest_blob.download_as_text())
    return data

def get_parse_manifest(upload_folder: str) -> dict:
    """
    Reads parse_manifest.json from GCS to resume or track status.
    """
    parse_manifest_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"{OUTPUT_PREFIX}{upload_folder}/parse_manifest.json"
    )
    if parse_manifest_blob.exists():
        return json.loads(parse_manifest_blob.download_as_text())
    # Default if no parse_manifest yet
    return {
        "lro_id": None,
        "status": "NEW",
        "start_time": None,
        "end_time": None,
        "error": None
    }

def save_parse_manifest(upload_folder: str, data: dict):
    """
    Writes the parse_manifest.json to GCS for checkpointing.
    """
    parse_manifest_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"{OUTPUT_PREFIX}{upload_folder}/parse_manifest.json"
    )
    parse_manifest_blob.upload_from_string(json.dumps(data, indent=2))

@Retry(initial=1.0, maximum=60.0, multiplier=2.0, deadline=600.0)
def start_lro(upload_folder: str, manifest_data: dict, parse_data: dict):
    """
    Starts a Long-Running Operation (Document AI batch processing) with auto-healing retry.

    Args:
        upload_folder (str): e.g. 'upload-164654'.
        manifest_data (dict): from manifest.json (file_names, etc.)
        parse_data (dict): parse_manifest.json data (lro_id, status, etc.)
    """
    # Build the array of docs with correct mime types
    docs = []
    for fname in manifest_data["file_names"]:
        doc_uri = f"gs://{SOURCE_BUCKET}/{SOURCE_PREFIX}{upload_folder}/{fname}"
        mime_type = guess_mime_type(fname)
        docs.append({"gcs_uri": doc_uri, "mime_type": mime_type})

    # v1beta3: snake_case -> input_documents -> gcs_documents -> documents
    input_documents = {
        "gcs_documents": {
            "documents": docs
        }
    }

    output_uri = f"gs://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}{upload_folder}/"

    # v1beta3: "document_output_config" -> "gcs_output_config"
    document_output_config = {
        "gcs_output_config": {
            "gcs_uri": output_uri
        }
    }

    # The name must end in :batchProcess for v1beta3
    name = f"projects/{PROJECT_ID}/locations/{REGION}/processors/{PROCESSOR_ID}:batchProcess"

    # Construct the request with correct field naming
    request = {
        "name": name,
        "input_documents": input_documents,          # The new snake_case field
        "document_output_config": document_output_config  # Also snake_case
    }

    logging.info(f"Starting LRO for {upload_folder} with {len(docs)} file(s).")

    try:
        # Start the LRO
        operation = documentai_client.batch_process_documents(request=request)
    except Exception as e:
        logging.error(f"Error calling batch_process_documents: {e}")
        raise

    lro_id = operation.operation.name
    parse_data["lro_id"] = lro_id
    parse_data["status"] = "IN_PROGRESS"
    parse_data["start_time"] = datetime.utcnow().isoformat()
    parse_data["end_time"] = None
    parse_data["error"] = None

    save_parse_manifest(upload_folder, parse_data)
    logging.info(f"LRO started: {lro_id}")

def check_lro_status(upload_folder: str, parse_data: dict):
    """
    Checks if the LRO is done, updates parse_manifest if success or failure.
    """
    lro_id = parse_data.get("lro_id")
    if not lro_id:
        logging.warning("No lro_id found, skipping status check.")
        return

    # Access the internal operations client to poll
    op = documentai_client._transport.operations_client.get_operation(lro_id)
    if not op.done:
        logging.info("LRO is still IN_PROGRESS.")
        return  # do nothing, still in progress

    if op.error.code != 0:
        parse_data["status"] = "FAILED"
        parse_data["end_time"] = datetime.utcnow().isoformat()
        parse_data["error"] = op.error.message
        save_parse_manifest(upload_folder, parse_data)
        logging.error(f"LRO failed: {op.error.message}")
        return

    # If no error, it's a success
    parse_data["status"] = "SUCCESS"
    parse_data["end_time"] = datetime.utcnow().isoformat()
    parse_data["error"] = None
    save_parse_manifest(upload_folder, parse_data)
    logging.info("LRO completed successfully.")

def combine_outputs(upload_folder: str):
    """
    (Optional) Combine or unify the doc AI outputs into unstructured_data.txt
    placeholder in:
    gs://OUTPUT_BUCKET/<OUTPUT_PREFIX>/<folder>/unstructured_data.txt
    """
    output_bucket = storage_client.bucket(OUTPUT_BUCKET)
    combined_text = "Placeholder text from doc AI outputs.\n"
    blob = output_bucket.blob(f"{OUTPUT_PREFIX}{upload_folder}/unstructured_data.txt")
    blob.upload_from_string(combined_text)
    logging.info("Created unstructured_data.txt as a placeholder.")

def main():
    """
    1. Reads manifest in source bucket
    2. Reads parse_manifest.json from output bucket
    3. If parse_manifest is NEW, start LRO.
    4. If parse_manifest is IN_PROGRESS, check LRO status.
    5. If parse_manifest is SUCCESS, skip or combine outputs.
    6. If parse_manifest is FAILED, handle logic or manual retry.
    """

    upload_folder = "upload-164654"  # Example or pass as an argument

    # 1) read source manifest
    manifest_data = read_manifest(upload_folder)
    logging.info(f"Loaded source manifest: {manifest_data}")

    # 2) read parse_manifest from output
    parse_data = get_parse_manifest(upload_folder)
    logging.info(f"Loaded parse_manifest: {parse_data}")

    status = parse_data["status"]
    if status == "NEW":
        logging.info("No LRO started yet, starting now.")
        start_lro(upload_folder, manifest_data, parse_data)
    elif status == "IN_PROGRESS":
        logging.info("LRO in progress, checking status...")
        check_lro_status(upload_folder, parse_data)
    elif status == "FAILED":
        logging.error("Previous run failed, handle logic or re-run.")
    elif status == "SUCCESS":
        logging.info("Already processed, optionally unify outputs or do chunking.")
        combine_outputs(upload_folder)
    else:
        logging.warning(f"Unknown status: {status}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
