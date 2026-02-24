import hashlib
import os
from pathlib import Path
from typing import Optional
from loguru import logger

def calculate_file_md5(file_path: str, chunk_size: int = 8192) -> Optional[str]:
    """
    Calculates the MD5 checksum of a local file to verify integrity.
    Uses chunked reading to handle large files (PDFs) without memory spikes.

    Args:
        file_path (str): Absolute or relative path to the file.
        chunk_size (int): Byte size for reading stream (default 8KB).

    Returns:
        str: The MD5 hash string (hexdigest) or None if operation fails.
    """
    path_obj = Path(file_path)

    # 1. Validation: Ensure file exists before attempting read
    if not path_obj.exists():
        logger.error(f"Cannot calculate hash: File not found at {file_path}")
        return None
    
    if not path_obj.is_file():
        logger.error(f"Cannot calculate hash: Path is not a file {file_path}")
        return None

    # 2. Hashing: Stream processing
    md5_hash = hashlib.md5()
    
    try:
        with open(path_obj, "rb") as f:
            while chunk := f.read(chunk_size):
                md5_hash.update(chunk)
        
        generated_hash = md5_hash.hexdigest()
        # logger.trace(f"MD5 calculated for {path_obj.name}: {generated_hash}")
        return generated_hash

    except OSError as e:
        logger.error(f"IO Error while hashing file {file_path}: {str(e)}")
        return None
    except Exception as e:
        logger.critical(f"Unexpected error in file hashing: {str(e)}")
        return None

def get_file_size_mb(file_path: str) -> float:
    """
    Returns file size in Megabytes (MB).
    Useful for logging and audit trails.
    """
    try:
        size_bytes = os.path.getsize(file_path)
        return round(size_bytes / (1024 * 1024), 2)
    except OSError:
        return 0.0

def sanitize_filename(filename: str) -> str:
    """
    Cleans a filename to ensure it is safe for the local filesystem.
    Removes characters that are illegal in Windows/Linux paths.
    
    Args:
        filename (str): The raw filename from Google Drive.
        
    Returns:
        str: A safe, sanitized filename.
    """
    # Keep only alphanumeric, dots, dashes, underscores, and spaces
    safe_name = "".join(c for c in filename if c.isalnum() or c in " ._-")
    return safe_name.strip()