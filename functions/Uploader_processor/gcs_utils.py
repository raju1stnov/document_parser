"""
gcs_utils.py

Handles interactions with Google Cloud Storage (GCS) and manifest creation.
"""

import os
import json
import time
from typing import List
from google.cloud import storage

def upload_to_gcs(local_paths, bucket_name, folder_prefix=None):
    """
    Uploads a file or list of files to GCS under the specified folder_prefix.

    Args:
        local_paths (str | List[str]): A single file path or a list of paths.
        bucket_name (str): The GCS bucket name.
        folder_prefix (str): e.g. 'abcd-1234/' to store in gs://bucket/abcd-1234/

    Returns:
        List[str]: List of GCS URIs.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    gcs_uris = []

    if isinstance(local_paths, str):
        local_paths = [local_paths]

    for local_path in local_paths:
        base_name = os.path.basename(local_path)  # e.g. chunk_0.pdf
        blob_name = f"{folder_prefix}{base_name}" if folder_prefix else base_name
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        gcs_uris.append(f"gs://{bucket_name}/{blob_name}")

    return gcs_uris

def create_manifest(original_filename: str,
                    chunk_uris: List[str],
                    folder_uuid: str,
                    bucket_name: str) -> str:
    """
    Creates a manifest.json in GCS with chunk info, timestamp, etc.

    Args:
        original_filename (str): The name of the original file.
        chunk_uris (List[str]): The GCS URIs of the chunked files.
        folder_uuid (str): Unique folder name in GCS (e.g. abcd-1234).
        bucket_name (str): GCS bucket for storing the manifest.

    Returns:
        str: GCS URI of the manifest file.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Build manifest data
    manifest = {
        "original_filename": original_filename,
        "num_chunks": len(chunk_uris),
        "chunk_files": [os.path.basename(uri) for uri in chunk_uris],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    manifest_json = json.dumps(manifest, indent=2)
    manifest_path = f"{folder_uuid}/manifest.json"

    blob = bucket.blob(manifest_path)
    blob.upload_from_string(manifest_json, content_type="application/json")

    return f"gs://{bucket_name}/{manifest_path}"