"""
gcs_utils.py

Handles interactions with Google Cloud Storage (GCS).
"""

import os
import json
from google.cloud import storage

def upload_to_gcs(local_paths, bucket_name, prefix=""):
    """
    Uploads a file or list of files to GCS.

    Args:
        local_paths (str | List[str]): A single file path or a list of paths.
        bucket_name (str): The target GCS bucket.
        prefix (str): Optional GCS prefix.

    Returns:
        List[str]: List of GCS URIs.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    gcs_uris = []

    if isinstance(local_paths, str):  # Single file case
        local_paths = [local_paths]

    for local_path in local_paths:
        blob_name = f"{prefix}{os.path.basename(local_path)}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        gcs_uris.append(f"gs://{bucket_name}/{blob_name}")

    return gcs_uris

def generate_manifest(original_filename, chunk_uris, upload_bucket):
    """
    Creates a JSON manifest file listing all chunked file URIs.

    Args:
        original_filename (str): The name of the original file.
        chunk_uris (List[str]): The GCS URIs of the chunked files.
        upload_bucket (str): The GCS bucket to store the manifest.

    Returns:
        str: GCS URI of the manifest file.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(upload_bucket)

    manifest = {
        "original_filename": original_filename,
        "num_chunks": len(chunk_uris),
        "chunk_uris": chunk_uris
    }

    manifest_json = json.dumps(manifest, indent=4)
    manifest_filename = f"manifests/{original_filename}.manifest.json"
    blob = bucket.blob(manifest_filename)
    blob.upload_from_string(manifest_json, content_type="application/json")

    return f"gs://{upload_bucket}/{manifest_filename}"