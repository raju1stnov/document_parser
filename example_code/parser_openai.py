import os
import time
import json
import logging
from google.cloud import storage
from google.api_core.client_options import ClientOptions
from google.cloud import documentai_v1beta3 as documentai
from google.api_core.retry import Retry

# Configuration
PROJECT_ID = "your-project-id"
LOCATION = "us"  # "us" or "us-central1"
PROCESSOR_ID = "your-processor-id"  # Layout Parser Processor
SOURCE_BUCKET = "my_bucket"
SOURCE_PREFIX = "source_path/upload-164654/"
DEST_BUCKET = "my_bucket"
DEST_PREFIX = "output_path/structured_data/upload-164654/"

# Initialize logging
logging.basicConfig(level=logging.INFO)

def guess_mime_type(filename: str) -> str:
    """Return appropriate MIME type for PDFs and Office files."""
    ext = os.path.splitext(filename)[1].lower()
    return {
        ".pdf": "application/pdf",
        ".tiff": "image/tiff",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".xls": "application/vnd.ms-excel",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".ppt": "application/vnd.ms-powerpoint",
        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }.get(ext, None)  # Returns None for unsupported types

def is_synchronous_supported(mime_type: str) -> bool:
    """Return True if the given mime_type is supported by process_document()."""
    return mime_type in ("application/pdf", "image/tiff", "image/jpeg", "image/png")

def process_documents():
    docai_client = documentai.DocumentProcessorServiceClient(
        client_options=ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
    )
    storage_client = storage.Client()

    # The layout parser processor name
    processor_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)

    # List files in source bucket
    source_bucket = storage_client.bucket(SOURCE_BUCKET)
    blobs = list(source_bucket.list_blobs(prefix=SOURCE_PREFIX))

    if not blobs:
        logging.info("No files found under prefix %s", SOURCE_PREFIX)
        return

    for blob in blobs:
        if blob.name.endswith("/"):  # skip directories
            continue

        file_name = os.path.basename(blob.name)
        dest_name = os.path.splitext(file_name)[0] + ".json"
        dest_path = os.path.join(DEST_PREFIX, dest_name)

        # If the .json output already exists, skip
        dest_blob = storage_client.bucket(DEST_BUCKET).blob(dest_path)
        if dest_blob.exists():
            logging.info(f"Skipping already processed: {file_name}")
            continue

        # Determine MIME type
        mime_type = guess_mime_type(file_name)
        if not mime_type:
            logging.info(f"Skipping unsupported file type: {file_name}")
            continue

        # Decide synchronous vs asynchronous
        if is_synchronous_supported(mime_type):
            logging.info(f"[SYNC] Processing {file_name} (Mime: {mime_type})")
            synchronous_process(docai_client, processor_name, blob, mime_type, dest_blob)
        else:
            logging.info(f"[ASYNC] Processing {file_name} (Mime: {mime_type})")
            asynchronous_process(docai_client, processor_name, blob, mime_type, dest_path)

def synchronous_process(docai_client, processor_name, blob, mime_type, dest_blob):
    """
    Use process_document(...) for PDF, TIFF, images.
    Immediately get Document object, upload JSON to GCS.
    """
    from google.cloud.documentai_v1beta3.types import ProcessRequest, RawDocument

    try:
        request = ProcessRequest(
            name=processor_name,
            raw_document=RawDocument(content=blob.download_as_bytes(), mime_type=mime_type)
        )
        result = docai_client.process_document(request=request)
        document = result.document

        # Save JSON to GCS
        dest_blob.upload_from_string(
            data=documentai.Document.to_json(document),
            content_type="application/json"
        )
        logging.info(f"Synchronous processing complete: {dest_blob.name}")

    except Exception as e:
        logging.error(f"Error processing {blob.name}: {e}")

@Retry(initial=1.0, maximum=60.0, multiplier=2.0, deadline=600.0)
def asynchronous_process(docai_client, processor_name, blob, mime_type, dest_path):
    """
    Use batch_process_documents(...) to handle docx, xlsx, pptx, etc.
    Polls until completion, then moves output JSON to <filename>.json.
    """
    from google.cloud.documentai_v1beta3.types import (
        BatchProcessRequest, GcsDocuments, GcsDocument, DocumentOutputConfig
    )
    storage_client = storage.Client()

    doc_uri = f"gs://{blob.bucket.name}/{blob.name}"
    output_gcs_uri = f"gs://{DEST_BUCKET}/{dest_path}.temp/"

    # Prepare batch request
    input_documents = GcsDocuments(documents=[GcsDocument(gcs_uri=doc_uri, mime_type=mime_type)])
    document_output_config = DocumentOutputConfig(
        gcs_output_config=DocumentOutputConfig.GcsOutputConfig(gcs_uri=output_gcs_uri)
    )

    request = BatchProcessRequest(
        name=processor_name + ":batchProcess",
        input_documents=BatchProcessRequest.InputDocuments(gcs_documents=input_documents),
        document_output_config=document_output_config
    )

    logging.info(f"Calling batch_process for {doc_uri}")
    operation = docai_client.batch_process_documents(request=request)
    op_name = operation.operation.name
    logging.info(f"Operation started: {op_name}")

    # Wait for completion
    operation.result()
    logging.info(f"Async processing completed for {blob.name}")

    # Locate the output JSON in temp folder
    output_bucket = storage_client.bucket(DEST_BUCKET)
    sub_blobs = list(output_bucket.list_blobs(prefix=dest_path + ".temp/"))

    if not sub_blobs:
        logging.error(f"No output found for {blob.name}")
        return

    # Find the first JSON file
    doc_json_blob = next((b for b in sub_blobs if b.name.endswith(".json")), None)
    if not doc_json_blob:
        logging.error(f"No .json file found in async output for {blob.name}")
        return

    # Move JSON to final location
    final_blob = output_bucket.blob(dest_path)
    final_blob.upload_from_string(doc_json_blob.download_as_text(), content_type="application/json")
    logging.info(f"Saved final JSON: {final_blob.name}")

    # Cleanup temp folder
    output_bucket.delete_blobs(sub_blobs)
    logging.info(f"Deleted temporary async output folder for {blob.name}")

def main():
    process_documents()

if __name__ == "__main__":
    main()