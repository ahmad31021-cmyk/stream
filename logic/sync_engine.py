import os
import re
import shutil
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Tuple

# Third-party libraries
from loguru import logger

# Internal modules
from services.drive_client import DriveClient
from services.openai_client import OpenAIClient
from database.db_manager import DBManager
from config.settings import settings

# Phase 2.5: Advanced Data Processing Modules
from services.extraction_service import ExtractionService
from logic.semantic_chunker import SemanticChunker
from utils.metadata_injector import MetadataInjector

class SyncEngine:
    """
    The main orchestrator responsible for synchronizing files between 
    Google Drive and OpenAI Vector Store.
    
    Implements a robust 'Delta Sync' strategy using MD5 checksums.
    Now utilizes asynchronous execution for massive-scale concurrent processing
    and integrates the Phase 2.5 Advanced Semantic Chunking Pipeline.
    """

    TEMP_DIR = Path("temp_data")

    def __init__(self):
        """
        Initializes the engine and its core service dependencies.
        """
        logger.info("Initializing Sync Engine...")
        
        # Initialize Service Connectors
        self.drive = DriveClient()
        self.openai = OpenAIClient()
        self.db = DBManager()

        # Initialize Phase 2.5 Intelligence Pipeline
        self.extractor = ExtractionService()
        self.chunker = SemanticChunker()
        self.injector = MetadataInjector()

        # Ensure temp directory exists and is clean
        self._prepare_temp_dir()

    def _prepare_temp_dir(self):
        """
        Ensures the temporary download directory exists and is empty.
        """
        if self.TEMP_DIR.exists():
            shutil.rmtree(self.TEMP_DIR)
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)

    async def start(self):
        """
        Main execution entry point.
        Orchestrates the full asynchronous synchronization workflow.
        """
        logger.info("Starting synchronization cycle...")

        try:
            # 1. Setup OpenAI Infrastructure (Vector Store) - Still sync, so keep to_thread
            vector_store_id = await asyncio.to_thread(self.openai.ensure_vector_store)
            logger.info(f"Connected to Vector Store ID: {vector_store_id}")
            
            # 2. Fetch remote file list from Google Drive - Now natively async
            logger.info("Fetching remote file list from Google Drive...")
            drive_files = await self.drive.list_pdfs_in_folder()
            
            # 3. Filter files that need processing (New or Changed) - Now natively async
            logger.info("Comparing remote files with local database state...")
            files_to_process = await self._get_files_to_process(drive_files)
            
            if not files_to_process:
                logger.success("System is up-to-date. No new files to sync.")
            else:
                logger.info(f"Identified {len(files_to_process)} files requiring synchronization.")
                await self._process_file_batch(files_to_process, vector_store_id)

            # 4. Finalize Assistant Configuration (Update Instructions/Tools) - Still sync
            logger.info("Updating Assistant Configuration...")
            assistant_id = await asyncio.to_thread(self.openai.ensure_assistant, vector_store_id)
            
            logger.success("Synchronization Cycle Completed Successfully.")

        except Exception as e:
            logger.critical(f"Synchronization failed due to critical error: {str(e)}")
            raise e
        finally:
            # Final cleanup of temp resources
            self._prepare_temp_dir()

    async def _get_files_to_process(self, drive_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Compares remote Drive files against the local database state.
        Returns a list of files that are either new or have a changed MD5 checksum.
        
        Args:
            drive_files: List of file objects from Google Drive API.
        
        Returns:
            List[Dict]: Subset of files needing upload.
        """
        processing_queue = []
        
        for file in drive_files:
            file_id = file['id']
            file_name = file['name']
            remote_checksum = file['md5Checksum']

            # Check DB state - Now natively async
            is_processed, stored_checksum = await self.db.check_file_status(file_id)

            if not is_processed:
                logger.info(f"New file detected: {file_name}")
                processing_queue.append(file)
            elif stored_checksum != remote_checksum:
                logger.warning(f"File modified (checksum mismatch): {file_name}. Re-syncing.")
                processing_queue.append(file)
            else:
                # File is up-to-date, skip
                pass

        return processing_queue

    def _parse_filename_metadata(self, filename: str) -> Tuple[str, str, str]:
        """
        Extracts Year, Author, and Title based on standard academic naming conventions.
        Format expected: "YYYY - Author - Title.pdf"
        Uses Regex to prevent corrupting metadata on non-standard filenames.
        """
        clean_name = filename.replace(".pdf", "").strip()
        
        # Regex explanation:
        # ^(?:(\d{4})\s*-\s*)? -> Optionally match exactly 4 digits at start (Year) followed by hyphen
        # (?:(.*?)\s*-\s*)?    -> Optionally match anything up to the next hyphen (Author)
        # (.*)$                -> Match everything else till the end (Title)
        pattern = r"^(?:(\d{4})\s*-\s*)?(?:(.*?)\s*-\s*)?(.*)$"
        match = re.match(pattern, clean_name)
        
        year = "Unknown"
        author = "Unknown"
        title = clean_name

        if match:
            extracted_year = match.group(1)
            extracted_author = match.group(2)
            extracted_title = match.group(3)

            if extracted_year:
                year = extracted_year.strip()
            
            if extracted_author:
                author = extracted_author.strip()
            
            if extracted_title:
                title = extracted_title.strip()
            elif extracted_author and not extracted_title:
                # Fallback if format is just "Author - Title" without year
                title = extracted_author.strip()
                author = "Unknown"
                
        logger.debug(f"Parsed Metadata -> Year: {year} | Author: {author} | Title: {title}")
        return year, author, title

    def _write_text_file(self, file_path: str, content: str):
        """
        Synchronous helper to write processed text to disk safely.
        """
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

    async def _process_single_file(self, file: Dict[str, Any], vector_store_id: str, semaphore: asyncio.Semaphore) -> bool:
        """
        Worker function to process a single file asynchronously.
        Ensures strict concurrency control via semaphore.
        Executes the Phase 2.5 Advanced Extraction & Chunking Pipeline.
        """
        file_id = file['id']
        file_name = file['name']
        checksum = file['md5Checksum']
        local_path = None
        processed_path = None

        async with semaphore:
            try:
                # Step A: Download from Drive (Now natively async, removed to_thread wrapper)
                logger.info(f"Downloading file: {file_name}")
                local_path = await self.drive.download_file(
                    file_id=file_id, 
                    file_name=file_name, 
                    destination_dir=str(self.TEMP_DIR)
                )

                if not local_path:
                    raise ValueError("Download returned no path.")

                # Step B: Phase 2.5 - Semantic Chunking & Metadata Injection Pipeline
                logger.info(f"Extracting & Chunking: {file_name}")
                year, author, title = self._parse_filename_metadata(file_name)
                
                # 1. Geometrically extract pages
                extracted_pages = await self.extractor.extract_document(local_path)
                logger.debug(f"Extracted {len(extracted_pages)} pages from {file_name}")
                
                final_text_blocks = []
                total_chunks = 0
                # 2. Semantically chunk each page and inject metadata
                for page in extracted_pages:
                    chunks = self.chunker.chunk_text(page["text"])
                    total_chunks += len(chunks)
                    for chunk in chunks:
                        meta = {
                            "title": title,
                            "author": author,
                            "year": year,
                            "internal_page_number": page["internal_page_number"]
                        }
                        enriched_chunk = self.injector.inject_metadata(chunk, meta)
                        final_text_blocks.append(enriched_chunk)

                final_document_text = "\n\n".join(final_text_blocks)
                logger.info(f"File {file_name} successfully processed into {total_chunks} chunks.")
                
                # 3. Save the highly structured output as a .txt file
                processed_path = f"{local_path}_processed.txt"
                await asyncio.to_thread(self._write_text_file, processed_path, final_document_text)

                # Step C: Upload the PROCESSED text file to OpenAI Vector Store
                logger.info(f"Uploading structured chunks to OpenAI Vector Store for: {file_name}")
                upload_status = await asyncio.to_thread(
                    self.openai.upload_file_to_store,
                    file_path=processed_path, 
                    vector_store_id=vector_store_id
                )

                # Step D: Update State in Database (Now natively async, removed to_thread wrapper)
                if upload_status:
                    await self.db.mark_file_as_processed(
                        file_id=file_id, 
                        file_name=file_name, 
                        checksum=checksum
                    )
                    logger.success(f"Successfully processed and recorded: {file_name}")
                    return True
                
            except Exception as e:
                logger.error(f"Failed to process {file_name}: {str(e)}")
                return False
            
            finally:
                # Step E: Immediate Cleanup of both original PDF and processed text
                pass
                # for path_to_clean in [local_path, processed_path]:
                #     if path_to_clean and os.path.exists(path_to_clean):
                #         try:
                #             os.remove(path_to_clean)
                #         except OSError:
                #             pass
        return False

    async def _process_file_batch(self, files: List[Dict[str, Any]], vector_store_id: str):
        """
        Iterates through the processing queue concurrently.
        Manages downloads -> uploads -> DB updates with strict concurrency limits.
        
        Args:
            files: List of files to process.
            vector_store_id: ID of the target OpenAI Vector Store.
        """
        # Concurrency limit to prevent RAM exhaustion and rate limits on massive datasets
        max_concurrent_tasks = 10
        semaphore = asyncio.Semaphore(max_concurrent_tasks)
        
        logger.info(f"Initiating concurrent batch processing with {max_concurrent_tasks} max workers...")

        # Create asynchronous tasks for all files
        tasks = [
            self._process_single_file(file, vector_store_id, semaphore)
            for file in files
        ]

        # Execute all tasks concurrently and wait for completion
        results = await asyncio.gather(*tasks)

        success_count = sum(1 for result in results if result is True)
        fail_count = len(files) - success_count

        logger.info(f"Batch Processing Summary: {success_count} Succeeded, {fail_count} Failed.")