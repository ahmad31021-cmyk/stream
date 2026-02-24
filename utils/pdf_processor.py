import os
import re
import asyncio
import fitz  # PyMuPDF: Industry standard for fast and accurate PDF parsing
from pathlib import Path
from typing import Optional, Tuple
from loguru import logger

# Internal modules
from utils.metadata_injector import MetadataInjector

class PDFProcessor:
    """
    Enterprise-grade PDF pre-processor.
    Extracts text page-by-page, attempts to identify internal pagination,
    and injects strict metadata blocks before the text reaches OpenAI's vector store.
    """

    def __init__(self):
        """
        Initializes the PDF Processor.
        """
        self.injector = MetadataInjector()

    async def process_pdf_for_vector_store(self, pdf_path: str, original_filename: str) -> Optional[str]:
        """
        Reads a raw PDF, processes it into a metadata-enriched text file, 
        and returns the path to the new text file.
        Runs purely in a background thread to prevent async loop blocking.

        Args:
            pdf_path (str): The local path to the downloaded PDF.
            original_filename (str): The name of the file to extract Author/Title/Year heuristics.

        Returns:
            Optional[str]: Path to the newly generated .txt file, or None if failed.
        """
        if not os.path.exists(pdf_path):
            logger.error(f"Cannot process PDF. File not found at: {pdf_path}")
            return None

        # Offload CPU-heavy PDF parsing to a separate thread
        processed_file_path = await asyncio.to_thread(self._extract_and_enrich, pdf_path, original_filename)
        return processed_file_path

    def _extract_and_enrich(self, pdf_path: str, original_filename: str) -> Optional[str]:
        """
        Synchronous worker function that handles the actual PyMuPDF processing.
        Generates a .txt file alongside the original PDF.
        """
        try:
            doc = fitz.open(pdf_path)
            
            # Basic heuristic to extract title and year from filename (e.g., "2009 - Mansell - Book.pdf")
            # In a full production system, this could query a master database.
            parsed_year, parsed_author, parsed_title = self._parse_filename_metadata(original_filename)

            output_text = ""

            for pdf_page_num in range(len(doc)):
                page = doc.load_page(pdf_page_num)
                raw_text = page.get_text("text").strip()

                if not raw_text:
                    continue  # Skip empty pages

                # Attempt to find the printed page number
                internal_page = self._guess_internal_page_number(raw_text)

                # Prepare metadata dictionary for the injector
                meta_dict = {
                    "title": parsed_title,
                    "author": parsed_author,
                    "year": parsed_year,
                    "internal_page_number": internal_page
                }

                # Inject strict Markdown block into this specific page's text
                enriched_page_text = self.injector.inject_metadata(raw_text, meta_dict)
                
                # Append to the final document payload
                output_text += enriched_page_text + "\n\n"

            doc.close()

            # Save the enriched text to a new file ready for OpenAI
            output_filepath = f"{pdf_path}_processed.txt"
            with open(output_filepath, "w", encoding="utf-8") as f:
                f.write(output_text)

            logger.info(f"Successfully processed and enriched PDF: {original_filename}")
            return output_filepath

        except Exception as e:
            logger.error(f"Failed to parse and enrich PDF {original_filename}: {str(e)}")
            return None

    def _guess_internal_page_number(self, page_text: str) -> str:
        """
        Heuristic method to find printed page numbers in headers or footers.
        Examines the first few and last few lines of the page text.
        """
        lines = page_text.split('\n')
        if not lines:
            return "Unknown"

        # Look at the top 3 and bottom 3 lines
        candidates = lines[:3] + lines[-3:]
        
        for line in candidates:
            clean_line = line.strip()
            # Regex to match standalone numbers (e.g., "59", "- 59 -", "Page 59")
            match = re.search(r'^(?:Page\s*)?-?\s*(\d+)\s*-?$', clean_line, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return "Unknown"

    def _parse_filename_metadata(self, filename: str) -> Tuple[str, str, str]:
        """
        Attempts to extract Year, Author, and Title from standard academic naming conventions.
        Format expected: "YYYY - Author - Title.pdf"
        """
        clean_name = filename.replace(".pdf", "").strip()
        parts = clean_name.split(" - ")
        
        year = "Unknown"
        author = "Unknown"
        title = clean_name

        if len(parts) >= 3:
            year = parts[0].strip()
            author = parts[1].strip()
            title = " - ".join(parts[2:]).strip()
        elif len(parts) == 2:
            year = parts[0].strip()
            title = parts[1].strip()

        return year, author, title