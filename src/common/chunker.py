"""
Document chunking utilities.
Intelligently splits large documents into processable chunks.
"""
import re
from typing import List, Dict
from dataclasses import dataclass, field


@dataclass
class Chunk:
    """Represents a document chunk."""
    content: str
    chunk_id: int
    start_pos: int
    end_pos: int
    metadata: Dict = None
    images: List[Dict] = field(default_factory=list)

    def to_dict(self):
        return {
            'content': self.content,
            'chunk_id': self.chunk_id,
            'start_pos': self.start_pos,
            'end_pos': self.end_pos,
            'metadata': self.metadata or {},
            'images': self.images or []
        }


class DocumentChunker:
    """Chunks documents intelligently by sections and size."""

    def __init__(self, chunk_size: int = 4000, overlap: int = 200):
        """
        Initialize chunker.

        Args:
            chunk_size: Target maximum characters per chunk
            overlap: Number of characters to overlap between chunks
        """
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk_document(self, text: str, filename: str = "") -> List[Chunk]:
        """
        Split document into chunks, preferring natural section boundaries.

        Args:
            text: Document text to chunk
            filename: Original filename for metadata

        Returns:
            List of Chunk objects
        """
        # Try to split by markdown headers first
        sections = self._split_by_sections(text)

        # If no sections found, split by paragraphs
        if len(sections) <= 1:
            sections = self._split_by_paragraphs(text)

        # Create chunks from sections
        chunks = []
        current_chunk = ""
        chunk_id = 0
        start_pos = 0

        for section in sections:
            # If adding this section exceeds chunk size and we have content, create a chunk
            if len(current_chunk) + len(section) > self.chunk_size and current_chunk:
                chunk = Chunk(
                    content=current_chunk.strip(),
                    chunk_id=chunk_id,
                    start_pos=start_pos,
                    end_pos=start_pos + len(current_chunk),
                    metadata={'filename': filename}
                )
                chunks.append(chunk)
                chunk_id += 1

                # Start new chunk with overlap
                overlap_text = current_chunk[-self.overlap:] if len(current_chunk) > self.overlap else current_chunk
                current_chunk = overlap_text + "\n\n" + section
                start_pos = start_pos + len(current_chunk) - len(overlap_text) - len(section) - 2
            else:
                current_chunk += ("\n\n" if current_chunk else "") + section

        # Add the last chunk
        if current_chunk.strip():
            chunk = Chunk(
                content=current_chunk.strip(),
                chunk_id=chunk_id,
                start_pos=start_pos,
                end_pos=start_pos + len(current_chunk),
                metadata={'filename': filename}
            )
            chunks.append(chunk)

        return chunks

    def _split_by_sections(self, text: str) -> List[str]:
        """Split text by markdown-style headers."""
        # Match markdown headers (# Header, ## Header, etc.)
        header_pattern = r'^#{1,6}\s+.+$'
        lines = text.split('\n')

        sections = []
        current_section = []

        for line in lines:
            if re.match(header_pattern, line, re.MULTILINE):
                # Start new section
                if current_section:
                    sections.append('\n'.join(current_section))
                current_section = [line]
            else:
                current_section.append(line)

        # Add last section
        if current_section:
            sections.append('\n'.join(current_section))

        return [s.strip() for s in sections if s.strip()]

    def _split_by_paragraphs(self, text: str) -> List[str]:
        """Split text by paragraphs (double newlines)."""
        paragraphs = re.split(r'\n\s*\n', text)
        return [p.strip() for p in paragraphs if p.strip()]
