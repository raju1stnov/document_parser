"""
main.py

Fault-tolerant script that:
1) Reads manifest.json from GCS (list of files).
2) For each file, calls Document AI Layout Parser (process_document) individually (NO LRO).
3) Extracts layout -> chunking strategy -> saves chunked data to GCS.
4) Maintains parse_manifest.json (array) with one entry per file, tracking status, timestamps, errors.

Folder structure:
gs://bucket_name/myname/source_path/
  upload-164654/
    my_test_pdf.pdf
    my_test_doc.docx
    my_test_excel.xls
    manifest.json

gs://bucket_name/myname/output_path/structured_data/
  upload-164654/
    unstructured_data/   (folder with chunk outputs, e.g. docx_chunk_001.txt, etc.)
    parse_manifest.json   (array tracking each file's parse status)
"""

import os
import json
import time
import logging
from datetime import datetime
from google.cloud import storage, documentai_v1beta3 as documentai
from google.api_core.retry import Retry

# ------------- Configuration -------------
SOURCE_BUCKET = "bucket_name"  # Where the user uploads raw files + manifest
SOURCE_PREFIX = "myname/source_path/"  # e.g. "myname/source_path/"

OUTPUT_BUCKET = "bucket_name"  # Where we write parse results
OUTPUT_PREFIX = "myname/output_path/structured_data/"  # e.g. "myname/output_path/structured_data/"

PROJECT_ID = "your-gcp-project-id"
REGION = "us"  # e.g. 'us' or 'us-central1'
PROCESSOR_ID = "6780"  # Document AI Layout Parser processor ID

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
    E.g.:
    {
      "no_files": "3",
      "file_names": ["my_test_pdf.pdf","my_test_doc.docx","my_test_excel.xls"],
      "timestamp": "..."
    }
    """
    manifest_blob = storage_client.bucket(SOURCE_BUCKET).blob(
        f"{SOURCE_PREFIX}{upload_folder}/manifest.json"
    )
    if not manifest_blob.exists():
        raise FileNotFoundError(f"manifest.json not found for {upload_folder}.")

    data = json.loads(manifest_blob.download_as_text())
    return data

def load_parse_manifest(upload_folder: str) -> list:
    """
    Reads parse_manifest.json from GCS, which is an array of objects:
    [
      {
        "process_id": "...",
        "status": "IN_PROGRESS|SUCCESS|FAILED",
        "filename": "my_test_doc.docx",
        "start_time": "...",
        "end_time": "...",
        "error": "..."
      },
      ...
    ]

    Returns an empty list if no parse_manifest yet.
    """
    parse_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"{OUTPUT_PREFIX}{upload_folder}/parse_manifest.json"
    )
    if not parse_blob.exists():
        return []
    return json.loads(parse_blob.download_as_text())

def save_parse_manifest(upload_folder: str, parse_list: list):
    """
    Writes the parse_manifest.json (array) to GCS.
    """
    parse_blob = storage_client.bucket(OUTPUT_BUCKET).blob(
        f"{OUTPUT_PREFIX}{upload_folder}/parse_manifest.json"
    )
    parse_blob.upload_from_string(json.dumps(parse_list, indent=2))

def get_file_entry(parse_list: list, filename: str) -> dict:
    """
    Finds an existing entry in parse_manifest for 'filename'.
    Returns None if not found.
    """
    for entry in parse_list:
        if entry["filename"] == filename:
            return entry
    return None

def chunk_layout(document) -> list:
    """
    This function demonstrates a simple context-aware chunking strategy
    using the layout information from Document AI. 
    We'll parse paragraphs or lines from `document.pages` for demonstration.
    
    Returns a list of chunk strings.
    """
    chunks = []
    for page in document.pages:
        # For each paragraph or layout element, accumulate text in a chunk
        text_chunks = []
        for paragraph in page.paragraphs:
            # paragraph.layout has bounding_poly, confidence, etc.
            # We'll extract paragraph text from 'document.text'
            start_index = paragraph.layout.text_anchor.text_segments[0].start_index
            end_index = paragraph.layout.text_anchor.text_segments[0].end_index
            text_chunks.append(document.text[start_index:end_index])
        if text_chunks:
            # Combine paragraphs for the page
            page_text = "\n\n".join(text_chunks)
            # This is one chunk for the entire page or combine smaller lumps
            chunks.append(page_text.strip())
    return chunks

@Retry(initial=1.0, maximum=60.0, multiplier=2.0, deadline=600.0)
def process_single_file(upload_folder: str, filename: str, parse_list: list):
    """
    Processes ONE file with Document AI Layout Parser.
    1) Looks up or creates an entry in parse_list
    2) If status is SUCCESS, skip
    3) Otherwise calls doc AI
    4) On success or fail, updates parse_list entry
    5) Extract layout -> chunking -> store chunked results in GCS
    """
    # Locate or create parse entry
    entry = get_file_entry(parse_list, filename)
    if not entry:
        entry = {
            "process_id": None,
            "status": "NEW",
            "filename": filename,
            "start_time": None,
            "end_time": None,
            "error": None
        }
        parse_list.append(entry)

    if entry["status"] == "SUCCESS":
        logging.info(f"{filename} already processed successfully, skipping.")
        return

    # Mark as IN_PROGRESS
    entry["status"] = "IN_PROGRESS"
    entry["start_time"] = datetime.utcnow().isoformat()
    entry["end_time"] = None
    entry["error"] = None
    entry["process_id"] = f"projects/{PROJECT_ID}/locations/{REGION}/processors/{PROCESSOR_ID}"

    save_parse_manifest(upload_folder, parse_list)

    # Prepare doc AI request
    doc_uri = f"gs://{SOURCE_BUCKET}/{SOURCE_PREFIX}{upload_folder}/{filename}"
    mime_type = guess_mime_type(filename)

    # We do single doc parse, NOT batch
    name = f"projects/{PROJECT_ID}/locations/{REGION}/processors/{PROCESSOR_ID}"

    request = {
        "name": name,
        "raw_document": {
            "gcs_uri": doc_uri,
            "mime_type": mime_type
        }
    }

    try:
        result = documentai_client.process_document(request=request)
    except Exception as e:
        logging.error(f"Error calling process_document for {filename}: {e}")
        entry["status"] = "FAILED"
        entry["end_time"] = datetime.utcnow().isoformat()
        entry["error"] = str(e)
        save_parse_manifest(upload_folder, parse_list)
        return

    # If doc AI call succeeded, do chunking of layout
    logging.info(f"Parsing layout for {filename} ...")
    doc = result.document  # A Document type
    # We'll chunk using our custom chunk_layout
    chunks = chunk_layout(doc)
    
    # Save each chunk to GCS
    output_folder = f"{OUTPUT_PREFIX}{upload_folder}/unstructured_data/"
    output_bucket = storage_client.bucket(OUTPUT_BUCKET)
    if chunks:
        for i, chunk_text in enumerate(chunks, start=1):
            chunk_path = f"{output_folder}{filename}_chunk_{i:03}.txt"
            blob = output_bucket.blob(chunk_path)
            blob.upload_from_string(chunk_text)
    else:
        # If no text or no layout, store empty
        chunk_path = f"{output_folder}{filename}_chunk_001.txt"
        blob = output_bucket.blob(chunk_path)
        blob.upload_from_string("No layout text extracted.")

    # Mark success
    entry["status"] = "SUCCESS"
    entry["end_time"] = datetime.utcnow().isoformat()
    entry["error"] = None

    save_parse_manifest(upload_folder, parse_list)
    logging.info(f"Processed {filename} successfully with {len(chunks)} chunks.")


def main():
    """
    1) Read manifest in source bucket
    2) Read parse_manifest.json from output
    3) For each file in the manifest, do process_single_file
    4) Summarize results
    """

    logging.basicConfig(level=logging.INFO)
    upload_folder = "upload-164654"  # example

    # 1) read source manifest
    manifest_data = read_manifest(upload_folder)
    file_names = manifest_data["file_names"]
    logging.info(f"Loaded source manifest with files: {file_names}")

    # 2) read parse_manifest from output
    parse_list = load_parse_manifest(upload_folder)

    # 3) for each file, do process_single_file
    for fname in file_names:
        process_single_file(upload_folder, fname, parse_list)

    # 4) Summarize
    logging.info("Final parse_manifest:")
    for entry in parse_list:
        logging.info(f"   {entry}")

    # Save final parse_list
    save_parse_manifest(upload_folder, parse_list)

if __name__ == "__main__":
    main()
