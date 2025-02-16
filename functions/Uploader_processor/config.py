"""
config.py

Stores configuration constants for chunk processing.
"""
import os

# Splitting threshold (MB) for chunking
CHUNK_SIZE_MB = int(os.getenv("CHUNK_SIZE_MB", "50"))

# The GCS source bucket where final files/chunks go
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "your-source-bucket")