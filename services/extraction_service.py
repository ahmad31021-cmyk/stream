import os
import re
import asyncio
import fitz  # PyMuPDF
from typing import List, Dict, Any
from loguru import logger

class ExtractionService:
    """
    Enterprise-grade PDF Text Extractor.
    Handles multi-column layouts, block sorting, and garbage filtering 
    found in complex maritime legal documents.
    """

    def __init__(self):
        # Thresholds for filtering out noisy blocks (like page numbers in weird places or watermarks)
        self.min_block_chars = 10

    async def extract_document(self, pdf_path: str) -> List[Dict[str, Any]]:
        """
        Asynchronously extracts structured text and page metadata from a PDF.
        Offloads the heavy CPU-bound PyMuPDF parsing to a background thread.

        Args:
            pdf_path (str): Local path to the PDF file.

        Returns:
            List[Dict]: A list of dictionaries representing pages. 
                        Format: [{"page_index": 0, "text": "...", "internal_page_number": "..."}]
        """
        if not os.path.exists(pdf_path):
            logger.error(f"Extraction failed. File not found: {pdf_path}")
            raise FileNotFoundError(f"Missing file for extraction: {pdf_path}")

        logger.info(f"Starting advanced block-level extraction for {os.path.basename(pdf_path)}")
        return await asyncio.to_thread(self._process_pdf_blocks, pdf_path)

    def _process_pdf_blocks(self, pdf_path: str) -> List[Dict[str, Any]]:
        """
        Synchronous worker that performs geometrical block-level text extraction.
        """
        extracted_pages = []
        try:
            doc = fitz.open(pdf_path)

            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                
                # Extract text as blocks: (x0, y0, x1, y1, "text", block_no, block_type)
                # block_type 0 means text, 1 means image
                blocks = page.get_text("blocks")
                
                # Filter out image blocks and extremely short garbage blocks
                text_blocks = [b for b in blocks if b[6] == 0 and len(b[4].strip()) > self.min_block_chars]
                
                # Sort blocks top-to-bottom, then left-to-right to accurately read multi-column academic papers
                # Round coordinates to group slightly misaligned lines properly
                text_blocks.sort(key=lambda b: (round(b[1] / 10) * 10, round(b[0] / 10) * 10))

                page_text = "\n\n".join([b[4].strip() for b in text_blocks])

                # Heuristic internal page extraction from the sorted text
                internal_page = self._guess_internal_pagination(page_text)

                extracted_pages.append({
                    "page_index": page_num,
                    "text": page_text,
                    "internal_page_number": internal_page
                })

            doc.close()
            return extracted_pages

        except Exception as e:
            logger.error(f"PyMuPDF Extraction Error on {pdf_path}: {str(e)}")
            raise e

    def _guess_internal_pagination(self, page_text: str) -> str:
        """
        Detects standalone page numbers usually found at the very top or bottom blocks.
        """
        lines = page_text.split('\n')
        if not lines:
            return "Unknown"

        candidates = lines[:5] + lines[-5:]
        for line in candidates:
            clean_line = line.strip()
            # Matches formats like "12", "- 12 -", "Page 12"
            match = re.search(r'^(?:Page\s*)?-?\s*(\d+)\s*-?$', clean_line, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return "Unknown"