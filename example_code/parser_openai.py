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
PROCESSOR_ID = "your-processor-id"  # Layout Parser
SOURCE_BUCKET = "my_bucket"
SOURCE_PREFIX = "source_path/upload-164654/"
DEST_BUCKET = "my_bucket"
DEST_PREFIX = "output_path/structured_data/upload-164654/"

# Initialize logging
logging.basicConfig(level=logging.INFO)

def guess_mime_type(filename: str) -> str:
    """Return appropriate MIME type for PDF or known Office extensions."""
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".pdf":
        return "application/pdf"
    elif ext == ".tiff":
        return "image/tiff"
    elif ext == ".jpg" or ext == ".jpeg":
        return "image/jpeg"
    elif ext == ".png":
        return "image/png"
    elif ext == ".doc" or ext == ".docx":
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    elif ext == ".xls" or ext == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif ext == ".ppt" or ext == ".pptx":
        return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    else:
        return None  # not supported

def is_synchronous_supported(mime_type: str) -> bool:
    """Return True if the given mime_type is supported by process_document()."""
    # According to Document AI docs, synchronous parsing (process_document)
    # supports PDF, TIFF, images. We'll treat them as sync.
    return mime_type in (
        "application/pdf",
        "image/tiff",
        "image/jpeg",
        "image/png"
    )

def process_documents():
    docai_client = documentai.DocumentProcessorServiceClient(
        client_options=ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
    )
    storage_client = storage.Client()

    # The layout parser processor name (NO :batchProcess, just the base)
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
            # Synchronous approach
            logging.info(f"[SYNC] Processing {file_name} with mime: {mime_type}")
            synchronous_process(docai_client, processor_name, blob, mime_type, dest_blob)
        else:
            # Asynchronous approach for docx, xlsx, pptx, etc.
            logging.info(f"[ASYNC] Processing {file_name} with mime: {mime_type}")
            asynchronous_process(docai_client, processor_name, blob, mime_type, dest_path)

def synchronous_process(docai_client, processor_name, blob, mime_type, dest_blob):
    """
    Use process_document(...) for PDF, TIFF, images, etc.
    Immediately get Document object, upload JSON to GCS as dest_blob.
    """
    from google.cloud.documentai_v1beta3.types import ProcessRequest, RawDocument

    # Download file into memory
    content = blob.download_as_bytes()
    request = ProcessRequest(
        name=processor_name,
        raw_document=RawDocument(content=content, mime_type=mime_type)
    )

    try:
        result = docai_client.process_document(request=request)
        document = result.document
        json_str = documentai.Document.to_json(document)
        dest_blob.upload_from_string(data=json_str, content_type="application/json")
        logging.info(f"Synchronous parse done. Uploaded {dest_blob.name}")
    except Exception as e:
        logging.error(f"Synchronous parse error for {blob.name}: {e}")

@Retry(initial=1.0, maximum=60.0, multiplier=2.0, deadline=600.0)
def asynchronous_process(docai_client, processor_name, blob, mime_type, dest_path):
    """
    Use batch_process_documents(...) with a single doc to parse e.g. DOCX, XLSX.
    Poll the operation, then rename the output JSON to our desired <filename>.json.
    """
    from google.cloud.documentai_v1beta3.types import (
        BatchProcessRequest,
        BatchProcessRequest_InputDocuments,
        BatchProcessRequest_DocumentOutputConfig,
        GcsDocuments,
        GcsDocument,
    )
    storage_client = storage.Client()

    # We'll do a single-document batch process for doc/docx/xls/xlsx
    doc_uri = f"gs://{blob.bucket.name}/{blob.name}"

    # Output folder for doc AI - it auto-writes subfolders
    # We'll choose a unique subfolder. For example:
    operation_folder = dest_path + ".temp"  # e.g. "upload-164654/my_test_doc.json.temp"
    output_gcs_uri = f"gs://{DEST_BUCKET}/{operation_folder}"

    # Build the request
    gcs_doc = GcsDocument(gcs_uri=doc_uri, mime_type=mime_type)
    gcs_docs = GcsDocuments(documents=[gcs_doc])
    input_docs = BatchProcessRequest_InputDocuments(gcs_documents=gcs_docs)

    doc_output_config = BatchProcessRequest_DocumentOutputConfig(
        gcs_output_config=BatchProcessRequest_DocumentOutputConfig.GcsOutputConfig(
            gcs_uri=output_gcs_uri
        )
    )

    request = BatchProcessRequest(
        name=processor_name + ":batchProcess",  # we do need :batchProcess for an async
        input_documents=input_docs,
        document_output_config=doc_output_config,
    )

    logging.info(f"Calling batch_process for {doc_uri}")
    operation = docai_client.batch_process_documents(request=request)

    op_name = operation.operation.name
    logging.info(f"Operation started: {op_name}")

    # Wait for operation done
    # This uses a direct approach
    operation.result()  # blocks until done or raises error
    logging.info(f"Async parse completed for {blob.name} -> folder {operation_folder}")

    # Now read the auto-generated JSON from that folder, rename to <filename>.json
    # Document AI typically writes them under something like:
    #   <operation_folder>/0/ or <operation_folder>/1/
    # Let's search sub-blobs
    output_bucket = storage_client.bucket(DEST_BUCKET)
    sub_blobs = list(output_bucket.list_blobs(prefix=operation_folder))

    if not sub_blobs:
        logging.error(f"No output found under {operation_folder}")
        return

    # Typically there's a JSON with "document.json"
    # We'll find the first "document.json" or *.json
    doc_json_blob = None
    for b in sub_blobs:
        if b.name.endswith(".json"):
            doc_json_blob = b
            break

    if not doc_json_blob:
        logging.error(f"No .json in {operation_folder}")
        return

    # Download that JSON
    json_str = doc_json_blob.download_as_text()

    # Finally, upload that to our final <filename>.json path (dest_path)
    final_blob = output_bucket.blob(dest_path)
    final_blob.upload_from_string(json_str, content_type="application/json")
    logging.info(f"Asynchronous parse done. Wrote final JSON -> {final_blob.name}")

    # Cleanup the temp folder if desired
    # for b in sub_blobs:
    #    b.delete()
    # output_bucket.delete_blobs(sub_blobs)
    # logging.info(f"Cleaned up temp folder {operation_folder}")

def main():
    process_documents()

if __name__ == "__main__":
    main()