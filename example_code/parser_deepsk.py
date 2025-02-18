import os
from google.cloud import documentai, storage
from google.api_core.client_options import ClientOptions

# Configuration
PROJECT_ID = "your-project-id"
LOCATION = "us"  # Update if different
PROCESSOR_ID = "your-processor-id"  # Create processor in Cloud Console
SOURCE_BUCKET = "my_bucket"
SOURCE_PREFIX = "source_path/upload-164654/"
DEST_BUCKET = "my_bucket"
DEST_PREFIX = "output_path/structured_data/upload-164654/"

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

    for blob in blobs:
        if blob.name.endswith("/"):  # Skip directories
            continue

        # Get file info
        file_path = blob.name
        file_name = os.path.basename(file_path)
        dest_name = os.path.splitext(file_name)[0] + ".json"
        dest_path = os.path.join(DEST_PREFIX, dest_name)

        # Skip already processed files
        dest_blob = storage_client.bucket(DEST_BUCKET).blob(dest_path)
        if dest_blob.exists():
            print(f"Skipping already processed: {file_name}")
            continue

        # Determine MIME type
        file_ext = os.path.splitext(file_name)[1].lower()
        mime_type = {
            ".pdf": "application/pdf",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }.get(file_ext)

        if not mime_type:
            print(f"Skipping unsupported file type: {file_name}")
            continue

        # Process document
        try:
            print(f"Processing {file_name}...")
            request = documentai.ProcessRequest(
                name=processor_name,
                raw_document=documentai.RawDocument(
                    content=blob.download_as_bytes(),
                    mime_type=mime_type,
                ),
            )

            result = docai_client.process_document(request)
            document = result.document

            # Save to destination bucket
            dest_blob.upload_from_string(
                data=documentai.Document.to_json(document),
                content_type="application/json",
            )
            print(f"Saved {dest_name}")

        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")

if __name__ == "__main__":
    process_documents()