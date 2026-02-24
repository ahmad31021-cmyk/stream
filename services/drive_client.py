import os
import io
import asyncio
import aiohttp
import aiofiles
from typing import List, Dict, Any, Optional
from pathlib import Path

# Third-party libraries
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger

# Internal modules
from config.settings import settings

class DriveClient:
    """
    A robust, enterprise-grade wrapper for the Google Drive API v3.
    Handles authentication, recursive file listing, and secure asynchronous HTTP downloading.
    """

    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

    def __init__(self):
        """
        Initializes the Drive Client.
        Validates credentials existence before attempting connection.
        """
        self.creds = None
        self.service = None
        self._authenticate()

    def _authenticate(self):
        """
        Authenticates using Service Account credentials defined in settings.
        Fail-fast strategy: If credentials are bad, crash immediately with a clear log.
        """
        try:
            if not os.path.exists(settings.GOOGLE_CREDENTIALS_FILE):
                raise FileNotFoundError(f"Credentials file not found at: {settings.GOOGLE_CREDENTIALS_FILE}")

            logger.info("Authenticating with Google Drive API...")
            self.creds = service_account.Credentials.from_service_account_file(
                settings.GOOGLE_CREDENTIALS_FILE, 
                scopes=self.SCOPES
            )
            self.service = build('drive', 'v3', credentials=self.creds, cache_discovery=False)
            logger.success("Google Drive Authentication Successful.")

        except Exception as e:
            logger.critical(f"Failed to authenticate with Google Drive: {str(e)}")
            raise e

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((HttpError, TimeoutError, ConnectionError)),
        reraise=True
    )
    async def list_pdfs_in_folder(self, folder_id: str = None) -> List[Dict[str, Any]]:
        """
        Recursively finds all PDF files within the specified folder and its subfolders.
        Uses thread offloading to prevent blocking the async event loop during deep scans.
        
        Args:
            folder_id (str): The Google Drive Folder ID to start searching from. 
                             Defaults to settings.GOOGLE_DRIVE_FOLDER_ID.

        Returns:
            List[Dict]: A list of file objects containing 'id', 'name', 'md5Checksum'.
        """
        target_folder_id = folder_id or settings.GOOGLE_DRIVE_FOLDER_ID
        if not target_folder_id:
            logger.error("No Google Drive Folder ID provided in settings.")
            raise ValueError("GOOGLE_DRIVE_FOLDER_ID is missing.")

        all_pdfs = []
        logger.info(f"Starting recursive scan for PDFs in folder ID: {target_folder_id}")

        # Offload the synchronous Google API discovery client walk to a background thread
        await asyncio.to_thread(self._walk_folder_tree, target_folder_id, all_pdfs)
        
        logger.info(f"Scan complete. Found {len(all_pdfs)} PDF files in total.")
        return all_pdfs

    def _walk_folder_tree(self, folder_id: str, pdf_accumulator: List[Dict[str, Any]]):
        """
        Internal helper to recursively walk the folder tree.
        Separates 'folders' for traversal and 'files' for collection.
        This remains synchronous but runs securely in a background thread.
        """
        page_token = None
        
        while True:
            try:
                # Query: Not trashed AND (is folder OR is PDF) AND parent is current folder
                query = (
                    f"'{folder_id}' in parents and trashed = false and "
                    f"(mimeType = 'application/vnd.google-apps.folder' or mimeType = 'application/pdf')"
                )
                
                results = self.service.files().list(
                    q=query,
                    pageSize=1000,
                    fields="nextPageToken, files(id, name, mimeType, md5Checksum)",
                    pageToken=page_token
                ).execute()

                items = results.get('files', [])

                for item in items:
                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        # Recursive step: Dive into subfolder
                        self._walk_folder_tree(item['id'], pdf_accumulator)
                    else:
                        # It is a PDF (based on query filter)
                        pdf_accumulator.append({
                            'id': item['id'],
                            'name': item['name'],
                            'md5Checksum': item.get('md5Checksum') # Crucial for sync logic
                        })

                page_token = results.get('nextPageToken')
                if not page_token:
                    break

            except HttpError as error:
                logger.error(f"An error occurred while scanning folder {folder_id}: {error}")
                raise error

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, ConnectionError, TimeoutError)),
        reraise=True
    )
    async def download_file(self, file_id: str, file_name: str, destination_dir: str) -> Optional[str]:
        """
        Downloads a specific file from Google Drive to the local temp directory.
        Upgraded to use an asynchronous HTTP client (aiohttp) and aiofiles 
        for background memory-efficient streaming of massive PDFs.

        Args:
            file_id (str): The Google Drive File ID.
            file_name (str): The name of the file (used for saving).
            destination_dir (str): Local path to save the file.

        Returns:
            str: The full local path of the downloaded file.
        """
        # Ensure credentials are valid and refreshed before requesting an access token
        if not self.creds.valid:
            await asyncio.to_thread(self.creds.refresh, Request())
            
        access_token = self.creds.token
        headers = {"Authorization": f"Bearer {access_token}"}
        
        # Direct REST API endpoint for media download
        url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"

        # Sanitize filename to prevent OS errors
        safe_filename = "".join([c for c in file_name if c.isalpha() or c.isdigit() or c in " ._-"])
        file_path = os.path.join(destination_dir, safe_filename)

        try:
            # Stream directly using async HTTP client to prevent RAM overload during 13GB ingestions
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    
                    # Asynchronous file writing
                    async with aiofiles.open(file_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(1024 * 1024): # 1MB chunks
                            await f.write(chunk)

            logger.info(f"Successfully downloaded via async stream: {file_name} -> {file_path}")
            return file_path

        except aiohttp.ClientError as e:
            logger.error(f"Async HTTP error downloading file {file_name} (ID: {file_id}): {e}")
            if os.path.exists(file_path):
                os.remove(file_path)
            raise e
        except Exception as e:
            logger.error(f"Unexpected error streaming {file_name}: {str(e)}")
            if os.path.exists(file_path):
                os.remove(file_path)
            raise e