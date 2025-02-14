"""
lro_status_checker/main.py

Cloud Function 3: Checks the Document AI LRO status for each chunk.
If complete, processes or merges final results.

Triggered by Cloud Scheduler, Pub/Sub, or a direct HTTP call.

Environment variables (example):
- PROJECT_ID, LOCATION, PROCESSOR_ID: doc AI details
- OUTPUT_BUCKET: GCS bucket with doc AI output
- CHUNK_BUCKET: The chunk bucket to scan for incomplete LROs
"""

import os
import json
import logging
from google.cloud import storage
from google.api_core import operation_pb2
from google.api_core import operation
from google.cloud import documentai_v1 as documentai

def check_lro_status(lro_id: str) -> bool:
    """
    Checks if the given LRO has completed.

    Args:
        lro_id (str): The long-running operation name.

    Returns:
        bool: True if done, False otherwise.
    """
    client = documentai.DocumentProcessorServiceClient()
    # Reconstruct the operation object
    op = operation.from_gapic(
        operation_pb2.Operation(name=lro_id),
        client._transport.operations_client,
        response_type=documentai.BatchProcessResponse
    )
    return op.done()

def merge_results_if_needed(lro_id: str):
    """
    Placeholder for optional post-processing:
    E.g., read doc AI output from GCS, merge with other chunk results, etc.
    """
    logging.info(f"[LROStatusChecker] LRO {lro_id} is done. Merging results (placeholder).")
    # Possibly read from gs://OUTPUT_BUCKET/docai_results/ and combine JSON/PDF

def handle_chunk_lro(blob):
    """
    Reads the LRO ID from chunk GCS metadata, checks status, merges if done.

    Args:
        blob (google.cloud.storage.Blob): The chunk file's Blob object.
    """
    metadata = blob.metadata or {}
    lro_id = metadata.get("lro_id")
    if not lro_id:
        return  # No LRO on this chunk

    is_done = check_lro_status(lro_id)
    if is_done:
        logging.info(f"[LROStatusChecker] LRO complete for {blob.name}")
        merge_results_if_needed(lro_id)
        # Optionally remove lro_id metadata or mark the chunk as processed
        metadata["lro_id"] = "DONE"
        blob.metadata = metadata
        blob.patch()
    else:
        logging.info(f"[LROStatusChecker] LRO still in progress for {blob.name}")

def poll_chunks(request):
    """
    HTTP trigger or scheduled trigger for LRO status check.
    Iterates over all chunk files in the chunk bucket, checks LRO status.

    Args:
        request (flask.Request): The incoming request (HTTP Cloud Function).
    """
    chunk_bucket_name = os.environ.get("CHUNK_BUCKET", "my-chunk-bucket")
    storage_client = storage.Client()
    bucket = storage_client.bucket(chunk_bucket_name)

    # List chunk files. You may filter or prefix-check, e.g., prefix='chunks/'
    for blob in bucket.list_blobs():
        handle_chunk_lro(blob)

    return "LRO status check complete", 200
