import os
from google.cloud import storage, documentai_v1 as documentai
import json

# Configuration
PROJECT_ID = "your-project-id"
LOCATION = "us"
PDF_PROCESSOR_ID = "pdf-processor-id"       # Processor for PDFs (sync)
BATCH_PROCESSOR_ID = "batch-processor-id"   # Processor for DOCX/XLSX (async)
SOURCE_BUCKET = "my_bucket"
SOURCE_PREFIX = "source_path/upload-164654"
TARGET_PREFIX = "output_path/structured_data/upload-164654"

storage_client = storage.Client()
documentai_client = documentai.DocumentProcessorServiceClient()

def process_pdf(blob):
    """Process PDF synchronously and upload structured data."""
    temp_path = f"/tmp/{blob.name.split('/')[-1]}"
    blob.download_to_filename(temp_path)

    with open(temp_path, "rb") as file:
        content = file.read()
    request = documentai.ProcessRequest(
        name=documentai_client.processor_path(PROJECT_ID, LOCATION, PDF_PROCESSOR_ID),
        raw_document=documentai.RawDocument(
            content=content,
            mime_type="application/pdf"
        )
    )
    response = documentai_client.process_document(request)
    structured_data = documentai.Document.to_json(response.document)
    
    os.remove(temp_path)
    
    # Upload to target path
    target_name = blob.name.replace(SOURCE_PREFIX, TARGET_PREFIX).replace(".pdf", ".json")
    blob.bucket.blob(target_name).upload_from_string(structured_data, content_type="application/json")

def process_docx_xlsx(blob):
    """Process DOCX/XLSX asynchronously and manage batch outputs."""
    original_base = blob.name.split("/")[-1].rsplit(".", 1)[0]
    input_uri = f"gs://{blob.bucket.name}/{blob.name}"
    output_uri = f"gs://{SOURCE_BUCKET}/{TARGET_PREFIX}/{original_base}/output/"
    
    # Submit batch job
    request = documentai.BatchProcessDocumentsRequest(
        name=documentai_client.processor_path(PROJECT_ID, LOCATION, BATCH_PROCESSOR_ID),
        documents=[
            {
                "mime_type": blob.content_type,
                "gcs_source": {"uri": input_uri}
            }
        ],
        output_config=documentai.DocumentOutputConfig(
            gcs_output_config=documentai.DocumentOutputConfig.GcsOutputConfig(
                gcs_uri=output_uri
            )
        )
    )
    operation = documentai_client.batch_process_documents(request)
    print(f"Started batch job for {input_uri}.")
    operation.result()  # Waits for completion
    
    # Process outputs
    output_bucket = storage_client.bucket(SOURCE_BUCKET)
    output_blobs = list(output_bucket.list_blobs(prefix=output_uri))
    
    merged_content = {}
    for blob in output_blobs:
        if blob.name.endswith(".json"):
            content = blob.download_as_bytes()
            merged_content.update(json.loads(content))  # Simplistic merge
    
    # Upload final merged JSON
    target_name = f"{TARGET_PREFIX}/structured_data/upload-164654/{original_base}.json"
    output_bucket.blob(target_name).upload_from_string(json.dumps(merged_content), content_type="application/json")

def main():
    bucket = storage_client.bucket(SOURCE_BUCKET)
    blobs = bucket.list_blobs(prefix=SOURCE_PREFIX)
    
    for blob in blobs:
        if blob.content_type == "application/pdf":
            process_pdf(blob)
        elif blob.content_type in ["application/vnd.openxmlformats-officedocument.wordprocessingml.document", 
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
            process_docx_xlsx(blob)
        else:
            print(f"Skipping unsupported file: {blob.name}")

if __name__ == "__main__":
    main()