import os
import uuid
from google.cloud import documentai, storage
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import RetryError

# Configuration
PROJECT_ID = "your-project-id"
LOCATION = "us"  # Update if different
PROCESSOR_ID = "your-processor-id"  # Document OCR Processor ID
SOURCE_BUCKET = "my_bucket"
SOURCE_PREFIX = "source_path/upload-164654/"
DEST_BUCKET = "my_bucket"
DEST_PREFIX = "output_path/structured_data/upload-164654/"
TEMP_ASYNC_PREFIX = "temp_async_output/"  # Temporary storage for async results

def process_documents():
    # Initialize clients
    docai_client = documentai.DocumentProcessorServiceClient(
        client_options=ClientOptions(
            api_endpoint=f"{LOCATION}-documentai.googleapis.com"
        )
    )
    storage_client = storage.Client()

    # Get processor name
    processor_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)

    # List files in source bucket
    source_bucket = storage_client.bucket(SOURCE_BUCKET)
    blobs = source_bucket.list_blobs(prefix=SOURCE_PREFIX)

    async_files = []  # To collect async processing documents

    for blob in blobs:
        if blob.name.endswith("/"):  # Skip directories
            continue

        file_path = blob.name
        file_name = os.path.basename(file_path)
        dest_name = os.path.splitext(file_name)[0] + ".json"
        dest_path = os.path.join(DEST_PREFIX, dest_name)

        # Skip already processed files
        dest_blob = storage_client.bucket(DEST_BUCKET).blob(dest_path)
        if dest_blob.exists():
            print(f"Skipping already processed: {file_name}")
            continue

        # Determine MIME type and processing type
        file_ext = os.path.splitext(file_name)[1].lower()
        mime_type = {
            ".pdf": "application/pdf",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }.get(file_ext)

        if not mime_type:
            print(f"Skipping unsupported file type: {file_name}")
            continue

        # Process PDFs synchronously
        if file_ext == ".pdf":
            try:
                print(f"Processing PDF {file_name}...")
                request = documentai.ProcessRequest(
                    name=processor_name,
                    raw_document=documentai.RawDocument(
                        content=blob.download_as_bytes(),
                        mime_type=mime_type,
                    ),
                )

                result = docai_client.process_document(request)
                dest_blob.upload_from_string(
                    data=documentai.Document.to_json(result.document),
                    content_type="application/json",
                )
                print(f"Saved {dest_name}")
            except Exception as e:
                print(f"Error processing {file_name}: {str(e)}")
        else:
            # Collect files for async processing
            async_files.append((file_path, mime_type, file_name))

    # Process DOCX/XLSX asynchronously
    if async_files:
        print(f"Processing {len(async_files)} async files...")
        batch_temp_prefix = f"{TEMP_ASYNC_PREFIX}{uuid.uuid4()}/"
        temp_uri = f"gs://{DEST_BUCKET}/{batch_temp_prefix}"

        # Prepare async request
        gcs_documents = []
        for file_path, mime_type, file_name in async_files:
            gcs_documents.append(
                documentai.GcsDocument(
                    gcs_uri=f"gs://{SOURCE_BUCKET}/{file_path}",
                    mime_type=mime_type
                )
            )

        try:
            # Batch process request
            request = documentai.BatchProcessRequest(
                name=processor_name,
                input_documents=documentai.BatchDocumentsInputConfig(
                    gcs_documents=documentai.GcsDocuments(documents=gcs_documents)
                ),
                document_output_config=documentai.DocumentOutputConfig(
                    gcs_output_config=documentai.GcsOutputConfig(
                        gcs_uri=temp_uri
                    )
                ),
            )

            operation = docai_client.batch_process_documents(request)
            print("Waiting for async processing to complete...")
            operation.result(timeout=3600)  # Wait up to 1 hour

            # Move results from temp to final destination
            source_bucket = storage_client.bucket(DEST_BUCKET)
            temp_blobs = source_bucket.list_blobs(prefix=batch_temp_prefix)

            for temp_blob in temp_blobs:
                if not temp_blob.name.endswith('.json'):
                    continue

                dest_file_name = os.path.basename(temp_blob.name)
                final_dest_path = os.path.join(DEST_PREFIX, dest_file_name)

                # Check if file already exists
                final_blob = source_bucket.blob(final_dest_path)
                if not final_blob.exists():
                    source_bucket.copy_blob(
                        temp_blob, source_bucket, final_dest_path
                    )
                    print(f"Moved {dest_file_name} to destination")
                
                # Cleanup temp file
                temp_blob.delete()

            print("Async processing completed successfully")

        except RetryError as e:
            print(f"Async processing timed out: {str(e)}")
        except Exception as e:
            print(f"Error during async processing: {str(e)}")

if __name__ == "__main__":
    process_documents()