import json
import os
import time
from datetime import datetime
from google.api_core.retry import Retry
from google.api_core.client_options import ClientOptions
from google.cloud import documentai, storage
from google.api_core.exceptions import GoogleAPICallError, RetryError

class DocumentAIProcessor:
    """Process documents using Document AI LRO with self-healing and state management."""
    
    def __init__(self, project_id: str, location: str, processor_id: str, bucket_name: str):
        self.project_id = project_id
        self.location = location
        self.processor_id = processor_id
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.docai_client = documentai.DocumentProcessorServiceClient(
            client_options=ClientOptions(
                api_endpoint=f"{location}-documentai.googleapis.com"
            )
        )
        self.processor_name = self.docai_client.processor_path(
            project_id, location, processor_id
        )

    @Retry(
        predicate=Retry.if_exception_type(GoogleAPICallError),
        initial=1.0,
        maximum=60.0,
        multiplier=2.0,
        deadline=600.0,
    )
    def process_upload_folder(self, upload_folder: str):
        """Process an upload folder with state management and auto-resume."""
        source_prefix = f"myname/source_path/{upload_folder}/"
        output_prefix = f"myname/output_path/structured_data/{upload_folder}/"
        
        # Check existing state
        state = self._load_parse_manifest(output_prefix)
        if state.get("status") == "SUCCESS":
            print(f"Already processed: {upload_folder}")
            return

        # Resume or start new processing
        if state.get("lro_id"):
            operation = self.docai_client.get_operation(name=state["lro_id"])
        else:
            file_uris = self._validate_manifest(source_prefix, upload_folder)
            operation = self._start_lro_processing(source_prefix, file_uris, output_prefix)
            self._update_parse_manifest(output_prefix, {
                "lro_id": operation.operation.name,
                "status": "IN_PROGRESS",
                "start_time": datetime.utcnow().isoformat()
            })

        # Monitor processing
        self._monitor_operation(operation, output_prefix)

    def _validate_manifest(self, source_prefix: str, upload_folder: str) -> list:
        """Validate manifest.json and return file URIs."""
        manifest_blob = self.storage_client.bucket(self.bucket_name).blob(
            f"{source_prefix}manifest.json"
        )
        
        try:
            manifest = json.loads(manifest_blob.download_as_text())
        except Exception as e:
            raise RuntimeError(f"Manifest validation failed: {str(e)}")

        file_uris = []
        for filename in manifest["file_names"]:
            blob = self.storage_client.bucket(self.bucket_name).blob(
                f"{source_prefix}{filename}"
            )
            if not blob.exists():
                raise FileNotFoundError(f"Missing file: {filename}")
            file_uris.append(f"gs://{self.bucket_name}/{blob.name}")

        return file_uris

    def _start_lro_processing(self, source_prefix: str, file_uris: list, output_prefix: str):
        """Start Document AI batch processing with retries."""
        input_config = documentai.BatchDocumentsInputConfig(
            gcs_documents=documentai.GcsDocuments(documents=[
                {"gcs_uri": uri, "mime_type": self._get_mime_type(uri)} for uri in file_uris
            ])
        )

        output_config = documentai.BatchDocumentsOutputConfig(
            gcs_output_config={"gcs_uri": f"gs://{self.bucket_name}/{output_prefix}"}
        )

        return self.docai_client.batch_process_documents(
            request=documentai.BatchProcessRequest(
                name=self.processor_name,
                input_configs=[input_config],
                output_config=output_config
            )
        )

    def _monitor_operation(self, operation, output_prefix: str):
        """Monitor LRO with state persistence and auto-resume."""
        try:
            while not operation.done():
                self._update_parse_manifest(output_prefix, {
                    "status": "IN_PROGRESS",
                    "last_checked": datetime.utcnow().isoformat()
                })
                time.sleep(30)
                operation = self.docai_client.get_operation(name=operation.name)

            if operation.error:
                raise GoogleAPICallError(f"LRO failed: {operation.error.message}")

            self._finalize_processing(output_prefix)
            
        except GoogleAPICallError as e:
            self._update_parse_manifest(output_prefix, {
                "status": "FAILED",
                "error": str(e)
            })
            raise

    def _finalize_processing(self, output_prefix: str):
        """Finalize processing and create output files."""
        self._generate_unstructured_data(output_prefix)
        self._update_parse_manifest(output_prefix, {
            "status": "SUCCESS",
            "end_time": datetime.utcnow().isoformat()
        })

    def _generate_unstructured_data(self, output_prefix: str):
        """Combine processed results into unstructured_data.txt."""
        bucket = self.storage_client.bucket(self.bucket_name)
        output_blobs = list(bucket.list_blobs(prefix=output_prefix))
        
        combined_text = []
        for blob in output_blobs:
            if blob.name.endswith(".json"):
                document = documentai.Document.from_json(blob.download_as_bytes())
                combined_text.append(document.text)

        output_blob = bucket.blob(f"{output_prefix}unstructured_data.txt")
        output_blob.upload_from_string("\n\n".join(combined_text))

    def _load_parse_manifest(self, output_prefix: str) -> dict:
        """Load parse manifest if exists."""
        blob = self.storage_client.bucket(self.bucket_name).blob(
            f"{output_prefix}parse_manifest.json"
        )
        try:
            return json.loads(blob.download_as_text())
        except:
            return {}

    def _update_parse_manifest(self, output_prefix: str, updates: dict):
        """Update parse manifest with new values."""
        manifest = self._load_parse_manifest(output_prefix)
        manifest.update(updates)
        
        blob = self.storage_client.bucket(self.bucket_name).blob(
            f"{output_prefix}parse_manifest.json"
        )
        blob.upload_from_string(json.dumps(manifest, indent=2))

    def _get_mime_type(self, gcs_uri: str) -> str:
        """Get MIME type based on file extension."""
        ext = os.path.splitext(gcs_uri)[1].lower()
        return {
            ".pdf": "application/pdf",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }.get(ext, "application/octet-stream")

def main():
    processor = DocumentAIProcessor(
        project_id="your-project-id",
        location="us",
        processor_id="your-processor-id",
        bucket_name="bucket_name"
    )

    # Find all upload folders
    blobs = processor.storage_client.list_blobs(
        "bucket_name", prefix="myname/source_path/upload-"
    )
    
    upload_folders = set()
    for blob in blobs:
        if "manifest.json" in blob.name:
            upload_folder = blob.name.split("/")[2]
            upload_folders.add(upload_folder)

    for folder in upload_folders:
        try:
            print(f"Processing {folder}")
            processor.process_upload_folder(folder)
        except Exception as e:
            print(f"Failed to process {folder}: {str(e)}")

if __name__ == "__main__":
    main()