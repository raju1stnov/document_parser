"""
chunker.py

Handles file chunking logic.
"""

import os

def split_file_into_chunks(local_path: str, chunk_size_mb: int = 20):
    """
    Splits a large file into smaller chunks of 20MB each.

    Args:
        local_path (str): Path to the file to be split.
        chunk_size_mb (int): Size of each chunk in MB.

    Returns:
        List[str]: Paths of created chunk files.
    """
    chunk_paths = []
    chunk_bytes = chunk_size_mb * 1024 * 1024

    with open(local_path, "rb") as infile:
        chunk_index = 0
        while True:
            data = infile.read(chunk_bytes)
            if not data:
                break
            chunk_filename = f"{os.path.basename(local_path)}.chunk{chunk_index}"
            chunk_path = f"/tmp/{chunk_filename}"
            with open(chunk_path, "wb") as chunk_file:
                chunk_file.write(data)
            chunk_paths.append(chunk_path)
            chunk_index += 1

    return chunk_paths