from typing import Dict, Any
from pydantic import BaseModel, Field, validator
from loguru import logger

class ChunkMetadata(BaseModel):
    """
    Pydantic schema to strictly validate and standardize the metadata 
    that will be injected into each text chunk. 
    Ensures no hallucination happens due to missing fields.
    """
    title: str = Field(default="Unknown", description="The title of the document or book.")
    author: str = Field(default="Unknown", description="The author(s) of the document.")
    year: str = Field(default="Unknown", description="The publication year.")
    internal_page_number: str = Field(
        default="Unknown", 
        description="The exact internal printed page number, not the PDF index."
    )

    @validator("internal_page_number", pre=True, always=True)
    def validate_page_number(cls, v):
        """Ensures that even if None is passed, it converts to 'Unknown'."""
        if not v or str(v).strip() == "":
            return "Unknown"
        return str(v).strip()

class MetadataInjector:
    """
    Enterprise-grade utility to securely inject structured metadata into raw text chunks.
    This guarantees that Vector Embeddings retain the exact internal page numbers and author data,
    preventing LLM hallucinations (e.g., confusing PDF Page 91 with Internal Page 59).
    """

    @staticmethod
    def inject_metadata(raw_chunk_text: str, metadata_dict: Dict[str, Any]) -> str:
        """
        Takes a raw chunk of text and appends a strictly formatted Markdown metadata block to the end.

        Args:
            raw_chunk_text (str): The raw text chunk extracted and segmented by SemanticChunker.
            metadata_dict (Dict[str, Any]): Dictionary containing 'title', 'author', 'year', 'internal_page_number'.

        Returns:
            str: The enriched text chunk ready for Vector Store uploading.
        """
        if not raw_chunk_text or not raw_chunk_text.strip():
            logger.warning("Attempted to inject metadata into an empty text chunk. Skipping.")
            return raw_chunk_text

        # 1. Validate incoming metadata strictly through Pydantic
        try:
            validated_meta = ChunkMetadata(**metadata_dict)
        except Exception as e:
            logger.error(f"Metadata validation failed. Using fallback 'Unknown' values. Error: {e}")
            validated_meta = ChunkMetadata()

        # 2. Sanitize the raw chunk slightly to ensure clean separation
        clean_text = raw_chunk_text.strip()

        # 3. Construct the strict Markdown block
        # This exact format trains the OpenAI Assistant to reliably extract these fields
        # when the RCH protocol is triggered.
        metadata_block = (
            "\n\n"
            "--------------------------------------------------\n"
            "**SOURCE METADATA FOR FORENSIC EXTRACTION:**\n"
            f"Title: {validated_meta.title}\n"
            f"Author: {validated_meta.author}\n"
            f"Year: {validated_meta.year}\n"
            f"Internal Pagination: {validated_meta.internal_page_number}\n"
            "--------------------------------------------------\n"
        )

        # 4. Append and return
        enriched_chunk = clean_text + metadata_block
        
        return enriched_chunk