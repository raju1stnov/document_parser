import os
from google.cloud import documentai_v1 as documentai
from google.cloud import storage
from google.api_core import exceptions
import itertools

# Configuration
PROJECT_ID = "your-project-id"
LOCATION = "us"  # Update if different
PDF_PROCESSOR_ID = "your-layout-processor-id"  # Processor for PDFs
BATCH_PROCESSOR_ID = "your-document-parser-id"  # Processor for DOCX/XLSX
SOURCE_BUCKET = "my_bucket"
SOURCE_PREFIX = "source_path/upload-164654"
DEST_BUCKET = "my_bucket"
DEST_PREFIX = "output_path/structured_data/upload-164654"

def main():
    storage_client = storage.Client()
    docai_client = documentai.DocumentProcessorServiceClient(
        client_options=documentai.DocumentProcessorServiceClient.create_client_options(
            api_endpoint=f"{LOCATION}-documentai.googleapis.com"
        )
    )

    source_bucket = storage_client.bucket(SOURCE_BUCKET)
    blobs = source_bucket.list_blobs(prefix=SOURCE_PREFIX)

    for blob in blobs:
        if blob.name.endswith('/'):
            continue  # Skip directories

        file_path = blob.name
        file_name = os.path.basename(file_path)
        document_id = os.path.splitext(file_name)[0]

        # Skip already processed files
        dest_blob = storage_client.bucket(DEST_BUCKET).blob(f"{DEST_PREFIX}/{document_id}.json")
        if dest_blob.exists():
            print(f"Skipping already processed: {file_name}")
            continue

        # Determine processor and MIME type
        if file_name.lower().endswith('.pdf'):
            process_pdf(docai_client, blob, document_id, DEST_PREFIX)
        elif file_name.lower().endswith(('.docx', '.xlsx')):
            process_docx_xlsx(docai_client, blob, document_id, DEST_PREFIX)
        else:
            print(f"Unsupported file type: {file_name}")

def process_pdf(docai_client, blob, document_id, dest_prefix):
    # Synchronous processing for PDF
    processor_name = docai_client.processor_path(PROJECT_ID, LOCATION, PDF_PROCESSOR_ID)
    request = documentai.ProcessRequest(
        name=processor_name,
        raw_document=documentai.RawDocument(
            content=blob.download_as_bytes(),
            mime_type='application/pdf'
        )
    )
    
    try:
        result = docai_client.process_document(request)
        structured_data = documentai.Document.to_json(result.document)
        upload_to_gcs(structured_data, document_id, dest_prefix)
    except exceptions.BadRequest as e:
        print(f"Error processing PDF {document_id}: {e}")

def process_docx_xlsx(docai_client, blob, document_id, dest_prefix):
    # Asynchronous batch processing for DOCX/XLSX
    processor_name = docai_client.processor_path(PROJECT_ID, LOCATION, BATCH_PROCESSOR_ID)
    temp_output_uri = f"gs://{SOURCE_BUCKET}/temp/{document_id}"
    
    # Prepare batch processing input
    input_config = documentai.BatchProcessDocumentInputConfig(
        gcs_document=documentai.GcsDocument(
            gcs_uri=f"gs://{blob.bucket.name}/{blob.name}",
            mime_type=blob.content_type
        )
    )
    request = documentai.BatchProcessRequest(
        name=processor_name,
        documents=[input_config],
        document_output_config=documentai.DocumentOutputConfig(
            gcs_output_config=documentai.DocumentOutputConfig.GcsOutputConfig(
                gcs_uri=temp_output_uri
            )
        )
    )
    
    try:
        operation = docai_client.batch_process_documents(request)
        operation.result()  # Wait for completion
        
        # Copy output to destination
        copy_output_to_destination(temp_output_uri, dest_prefix, document_id)
    except exceptions.BadRequest as e:
        print(f"Error processing {blob.name}: {e}")

def upload_to_gcs(json_data, document_id, dest_prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(DEST_BUCKET)
    dest_blob = bucket.blob(f"{dest_prefix}/{document_id}.json")
    dest_blob.upload_from_string(json_data, content_type='application/json')
    print(f"Saved {dest_prefix}/{document_id}.json")

def copy_output_to_destination(temp_output_uri, dest_prefix, document_id):
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(SOURCE_BUCKET)
    output_files = list(source_bucket.list_blobs(prefix=temp_output_uri))
    
    if not output_files:
        print(f"No output found for {document_id}")
        return
    
    # Handle multiple output files (e.g., from large documents)
    # Assuming the first JSON blob is the main structure
    main_output_blob = next((b for b in output_files if b.name.endswith('.json')), None)
    if not main_output_blob:
        print(f"Error: No JSON output found for {document_id}")
        return
    
    # Upload the generated JSON to the destination
    structured_data = main_output_blob.download_as_bytes()
    upload_to_gcs(structured_data, document_id, dest_prefix)
    
    # Clean up temporary files
    for blob in output_files:
        blob.delete()
    print(f"Temporary files deleted: {temp_output_uri}")

if __name__ == "__main__":
    main()