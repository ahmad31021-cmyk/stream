import os
from pathlib import Path
from typing import Optional, Dict, Any

# Third-party libraries
from openai import OpenAI, APIConnectionError, RateLimitError, APIError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger

# Internal modules
from config.settings import settings

class OpenAIClient:
    """
    Enterprise wrapper for OpenAI Assistants API v2.
    Manages the Assistant lifecycle, Vector Stores, and File synchronizations.
    """

    # Constants for strict enforcement
    MODEL_VERSION = "gpt-4o"
    ASSISTANT_NAME = "SCAPILE - Maritime Law Expert"
    VECTOR_STORE_NAME = "SCAPILE Knowledge Base"

    def __init__(self):
        """
        Initializes the OpenAI Client.
        Fails fast if the API Key is missing.
        """
        if not settings.OPENAI_API_KEY:
            logger.critical("OPENAI_API_KEY is missing in settings.")
            raise ValueError("OPENAI_API_KEY is required.")

        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.instructions_path = Path("assets/system_instructions.txt")
        
        logger.info(f"OpenAI Client initialized. Target Model: {self.MODEL_VERSION}")

    def _load_system_instructions(self) -> str:
        """
        Reads the 'Gold' instructions from the assets folder.
        This ensures the RCH Protocol & Hierarchy are always up to date.
        """
        try:
            if not self.instructions_path.exists():
                raise FileNotFoundError(f"System instructions file not found at: {self.instructions_path}")
            
            content = self.instructions_path.read_text(encoding="utf-8")
            if not content.strip():
                raise ValueError("System instructions file is empty.")
            
            return content
        except Exception as e:
            logger.critical(f"Failed to load system instructions: {str(e)}")
            raise e

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((APIConnectionError, RateLimitError, APIError))
    )
    def ensure_vector_store(self) -> str:
        """
        Checks if a Vector Store exists (via ID in settings or Name search).
        If not, creates a new one.
        
        Returns:
            str: The Vector Store ID (vs_...).
        """
        # 1. Check if ID is already configured in environment
        if settings.OPENAI_VECTOR_STORE_ID:
            try:
                # Verify it actually exists on OpenAI
                self.client.vector_stores.retrieve(settings.OPENAI_VECTOR_STORE_ID)
                logger.info(f"Using configured Vector Store ID: {settings.OPENAI_VECTOR_STORE_ID}")
                return settings.OPENAI_VECTOR_STORE_ID
            except Exception:
                logger.warning(f"Configured Vector Store ID {settings.OPENAI_VECTOR_STORE_ID} is invalid. Searching by name...")

        # 2. Search by Name to prevent duplicates
        vector_stores = self.client.vector_stores.list(limit=50)
        for store in vector_stores.data:
            if store.name == self.VECTOR_STORE_NAME:
                logger.success(f"Found existing Vector Store: {store.name} ({store.id})")
                return store.id

        # 3. Create New if not found
        logger.info(f"Creating new Vector Store: {self.VECTOR_STORE_NAME}")
        new_store = self.client.vector_stores.create(name=self.VECTOR_STORE_NAME)
        logger.success(f"Created new Vector Store with ID: {new_store.id}")
        
        # NOTE: In a full production env, we would write this ID back to .env or a DB.
        # For now, the Sync Engine will hold it in memory or user must update .env.
        return new_store.id

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=20),
        retry=retry_if_exception_type((APIConnectionError, RateLimitError))
    )
    def upload_file_to_store(self, file_path: str, vector_store_id: str) -> str:
        """
        Uploads a local file to the OpenAI Vector Store.
        Uses the 'upload_and_poll' helper for reliability.
        In Phase 2.5, this now expects pre-processed, chunked, and metadata-enriched
        text/markdown files rather than raw dense PDFs.

        Args:
            file_path (str): Local path to the processed text/markdown file.
            vector_store_id (str): Target Vector Store ID.

        Returns:
            str: The OpenAI File ID or success status.
        """
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File to upload not found: {file_path}")

        logger.info(f"Uploading structured, pre-processed file to OpenAI Vector Store: {file_path_obj.name}")

        try:
            # Create a file stream
            with open(file_path_obj, "rb") as file_stream:
                # Use the helper to upload and add to vector store in one go
                # This automatically handles the "awaiting_processing" state
                batch = self.client.vector_stores.file_batches.upload_and_poll(
                    vector_store_id=vector_store_id,
                    files=[file_stream]
                )
            
            if batch.status == "completed" and batch.file_counts.completed > 0:
                # Retrieve the file ID (since we uploaded a batch of 1, we fetch files from the store to get the ID)
                # Optimization: We return a success status. The caller often just needs to know it worked.
                # If exact ID is needed, we would list files in batch, but upload_and_poll returns a Batch object.
                logger.success(f"Successfully uploaded and indexed structured payload: {file_path_obj.name}")
                return "upload_success"
            else:
                raise APIError(f"File upload failed with status: {batch.status}")

        except Exception as e:
            logger.error(f"Failed to upload {file_path_obj.name} to Vector Store: {str(e)}")
            raise e

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((APIConnectionError, RateLimitError))
    )
    def ensure_assistant(self, vector_store_id: str) -> str:
        """
        Ensures the 'SCAPILE' Assistant exists with the correct configuration.
        Updates Instructions, Model, and Tools dynamically.

        Args:
            vector_store_id (str): The ID of the vector store to attach.

        Returns:
            str: The Assistant ID.
        """
        instructions = self._load_system_instructions()
        
        # 1. Check if Assistant ID is in settings
        assistant_id = settings.OPENAI_ASSISTANT_ID
        
        if assistant_id:
            try:
                # Update existing assistant to ensure compliance with new instructions/model
                logger.info(f"Updating existing Assistant ({assistant_id}) with latest protocols...")
                self.client.beta.assistants.update(
                    assistant_id=assistant_id,
                    name=self.ASSISTANT_NAME,
                    instructions=instructions,
                    model=self.MODEL_VERSION,
                    tools=[{"type": "file_search"}],
                    tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                )
                logger.success(f"Assistant {assistant_id} updated successfully.")
                return assistant_id
            except Exception as e:
                logger.warning(f"Could not update configured Assistant {assistant_id}: {e}. Searching by name...")

        # 2. Search by Name (if ID missing or invalid)
        my_assistants = self.client.beta.assistants.list(limit=20)
        for assistant in my_assistants.data:
            if assistant.name == self.ASSISTANT_NAME:
                logger.info(f"Found existing Assistant by name: {assistant.id}. Updating...")
                self.client.beta.assistants.update(
                    assistant_id=assistant.id,
                    instructions=instructions,
                    model=self.MODEL_VERSION,
                    tools=[{"type": "file_search"}],
                    tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                )
                return assistant.id

        # 3. Create New Assistant
        logger.info(f"Creating NEW Assistant: {self.ASSISTANT_NAME}")
        new_assistant = self.client.beta.assistants.create(
            name=self.ASSISTANT_NAME,
            instructions=instructions,
            model=self.MODEL_VERSION,
            tools=[{"type": "file_search"}],
            tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
        )
        logger.success(f"Created new Assistant with ID: {new_assistant.id}")
        return new_assistant.id