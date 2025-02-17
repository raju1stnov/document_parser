"""
main.py

Demonstration of a fault-tolerant Document AI LRO script.

Features:
1. Reads a 'manifest.json' from a GCS source folder (upload-<random>).
2. Starts or resumes a Document AI LRO using @Retry for transient failures.
3. Stores parse_manifest.json in GCS to track LRO status (IN_PROGRESS, SUCCESS, or FAILED).
4. If the script is re-run, it reads parse_manifest.json and decides whether to resume or skip.

Folder structure (example):
gs://bucket_name/myname/source_path/
    upload-164654/
        my_test_pdf.pdf
        my_test_doc.docx
        my_test_excel.xl
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
from google.cloud import storage, documentai_v1beta3 as documentai
from google.api_core.retry import Retry
from datetime import datetime

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

def read_manifest(upload_folder: str) -> dict:
    """
    Reads the manifest.json from GCS that lists the files to be processed.

    Args:
        upload_folder (str): e.g. 'upload-164654'.

    Returns:
        dict: The contents of manifest.json, e.g. {
            "no_files": "3",
            "file_names": ["my_test_pdf.pdf","my_test_doc.docx","my_test_excel.xl"],
            "timestamp": "..."
        }
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

    Args:
        upload_folder (str): e.g. 'upload-164654'.

    Returns:
        dict: The parse manifest containing LRO status or empty if doesn't exist.
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
    Writes the parse_manifest.json to GCS.

    Args:
        upload_folder (str): e.g. 'upload-164654'.
        data (dict): The parse manifest data to store.
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
        manifest_data (dict): The source manifest (file_names, no_files, etc.).
        parse_data (dict): parse_manifest.json data (lro_id, status, etc.).
    """
    # Build the inputDocuments for doc AI from the manifest
    # Each file is in gs://bucket_name/myname/source_path/upload-<folder>/<filename>
    documents = []
    for fname in manifest_data["file_names"]:
        doc_uri = f"gs://{SOURCE_BUCKET}/{SOURCE_PREFIX}{upload_folder}/{fname}"
        # We pass each file to doc AI for a combined batch
        documents.append({"gcs_uri": doc_uri, "mime_type": "application/pdf"})
        # If your files aren't pdf, doc, or excel, you may need separate logic or unify them to pdf

    input_documents = {"documents": documents}

    # Output GCS folder for structured data
    output_uri = f"gs://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}{upload_folder}/"

    request = {
        "name": f"projects/{PROJECT_ID}/locations/{REGION}/processors/{PROCESSOR_ID}:batchProcess",
        "inputDocuments": {
            "gcsDocuments": input_documents  # all files
        },
        "outputConfig": {
            "gcsDestination": {"uri": output_uri},
            "pagesPerShard": 10
        }
    }

    logging.info(f"Starting LRO for {upload_folder} with {len(documents)} docs.")
    operation = documentai_client.batch_process_documents(request)
    lro_id = operation.operation.name

    # Update parse_data
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

    Args:
        upload_folder (str): e.g. 'upload-164654'.
        parse_data (dict): parse_manifest data.
    """
    lro_id = parse_data.get("lro_id")
    if not lro_id:
        logging.warning("No lro_id found, skipping status check.")
        return

    # Convert the LRO name into an Operation object
    from google.api_core import operation
    from google.cloud.documentai_v1beta3.types import BatchProcessResponse
    from google.protobuf import any_pb2
    from google.api_core.protobuf_helpers import from_any

    op = documentai_client._transport.operations_client.get_operation(lro_id)
    if not op.done:
        logging.info("LRO is still IN_PROGRESS.")
        return  # do nothing, still in progress

    # If done, check for error or success
    if op.error.code != 0:
        # LRO failed
        parse_data["status"] = "FAILED"
        parse_data["end_time"] = datetime.utcnow().isoformat()
        parse_data["error"] = op.error.message
        save_parse_manifest(upload_folder, parse_data)
        logging.error(f"LRO failed: {op.error.message}")
        return

    # LRO success
    parse_data["status"] = "SUCCESS"
    parse_data["end_time"] = datetime.utcnow().isoformat()
    parse_data["error"] = None
    save_parse_manifest(upload_folder, parse_data)

    logging.info("LRO completed successfully.")

def combine_outputs(upload_folder: str):
    """
    (Optional) Combine or unify the doc AI outputs into unstructured_data.txt
    stored in:
    gs://OUTPUT_BUCKET/myname/output_path/structured_data/upload-<random>/unstructured_data.txt

    This step is a placeholder: you can parse the doc AI JSON, extract text,
    or do any post-processing you want.
    """
    # for demonstration, we just create a dummy unstructured_data.txt
    output_bucket = storage_client.bucket(OUTPUT_BUCKET)
    combined_text = "Placeholder text from doc AI outputs.\n"
    # If you want to parse doc AI JSON, you can read from
    # f"{OUTPUT_PREFIX}{upload_folder}/...some doc ai json..."
    # and unify them here.

    # Save unstructured_data
    blob = output_bucket.blob(f"{OUTPUT_PREFIX}{upload_folder}/unstructured_data.txt")
    blob.upload_from_string(combined_text)
    logging.info("Created unstructured_data.txt as a placeholder.")

def main():
    """
    Main entry:
    1. Reads manifest in source bucket
    2. Reads parse_manifest.json from output bucket
    3. If parse_manifest is NEW, start LRO.
    4. If parse_manifest is IN_PROGRESS, check LRO status.
    5. If parse_manifest is SUCCESS, skip or combine outputs.
    """

    upload_folder = "upload-164654"  # Example, or pass as an argument

    # 1) read source manifest
    manifest_data = read_manifest(upload_folder)
    logging.info(f"Loaded source manifest: {manifest_data}")

    # 2) read parse_manifest from output
    parse_data = get_parse_manifest(upload_folder)
    logging.info(f"Loaded parse_manifest: {parse_data}")

    status = parse_data["status"]
    if status == "NEW":
        # start LRO
        logging.info("No LRO started yet, starting now...")
        start_lro(upload_folder, manifest_data, parse_data)
    elif status == "IN_PROGRESS":
        # check if LRO done
        logging.info("LRO in progress, checking status...")
        check_lro_status(upload_folder, parse_data)
    elif status == "FAILED":
        logging.error("Previous run failed, handle logic or re-run.")
    elif status == "SUCCESS":
        logging.info("Already processed, optionally unify outputs or do chunking.")
        # Optionally combine or finalize data
        combine_outputs(upload_folder)
    else:
        logging.warning(f"Unknown status: {status}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
