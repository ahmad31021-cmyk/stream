import re
from typing import List
from loguru import logger

class SemanticChunker:
    """
    Intelligent Text Chunker designed for legal and academic documents.
    Prevents the fragmentation of clauses and sentences by strictly respecting 
    natural language boundaries (paragraphs and punctuation).
    """

    def __init__(self, max_chunk_chars: int = 3000):
        """
        Initializes the Semantic Chunker.
        
        Args:
            max_chunk_chars (int): The maximum safe character limit per chunk.
                                   3000 chars is roughly 600-800 tokens, ideal for embeddings.
        """
        self.max_chunk_chars = max_chunk_chars

    def chunk_text(self, text: str) -> List[str]:
        """
        Splits a large block of text into semantic chunks based on paragraphs and sentences.
        
        Args:
            text (str): The raw extracted text from a page or document.

        Returns:
            List[str]: A list of semantically intact text chunks.
        """
        if not text or not text.strip():
            return []

        chunks = []
        current_chunk = ""

        # Step 1: Split into natural paragraphs using double newlines as boundaries
        paragraphs = re.split(r'\n\s*\n', text.strip())

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            # If a single paragraph is larger than the max limit, we must split it by sentences
            if len(para) > self.max_chunk_chars:
                sentence_chunks = self._split_by_sentences(para)
                for sc in sentence_chunks:
                    if len(current_chunk) + len(sc) + 1 <= self.max_chunk_chars:
                        current_chunk += sc + " "
                    else:
                        if current_chunk:
                            chunks.append(current_chunk.strip())
                        current_chunk = sc + " "
            
            # If the paragraph fits, try to append it to the current chunk
            else:
                if len(current_chunk) + len(para) + 2 <= self.max_chunk_chars:
                    current_chunk += para + "\n\n"
                else:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                    current_chunk = para + "\n\n"

        # Push the final remaining chunk
        if current_chunk.strip():
            chunks.append(current_chunk.strip())

        logger.debug(f"Semantic Chunking produced {len(chunks)} intact chunks.")
        return chunks

    def _split_by_sentences(self, text: str) -> List[str]:
        """
        Breaks down a massive paragraph into sentences using regex boundary detection.
        Ensures quotes and acronyms don't completely break the logic.
        """
        # Regex explanation: Splits by ., !, or ? followed by a space and a capital letter, 
        # or the end of the string. Keeps the punctuation attached to the sentence.
        sentence_boundaries = re.compile(r'(?<=[.!?])\s+(?=[A-Z])')
        
        sentences = sentence_boundaries.split(text)
        return [s.strip() for s in sentences if s.strip()]