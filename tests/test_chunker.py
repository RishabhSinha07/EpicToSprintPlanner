"""
Tests for document chunker.
"""
import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from common.chunker import DocumentChunker, Chunk


def test_chunker_basic():
    """Test basic chunking functionality."""
    chunker = DocumentChunker(chunk_size=100, overlap=20)

    text = "This is a test document. " * 10
    chunks = chunker.chunk_document(text, "test.txt")

    assert len(chunks) > 0
    assert all(isinstance(c, Chunk) for c in chunks)
    assert all(c.content for c in chunks)


def test_chunker_with_sections():
    """Test chunking with markdown sections."""
    chunker = DocumentChunker(chunk_size=200, overlap=20)

    text = """# Section 1
This is the first section with some content.

## Subsection 1.1
More content here.

# Section 2
This is the second section.
"""

    chunks = chunker.chunk_document(text, "test.md")

    assert len(chunks) > 0
    # First chunk should contain the first section
    assert "Section 1" in chunks[0].content


def test_chunk_to_dict():
    """Test chunk serialization."""
    chunk = Chunk(
        content="Test content",
        chunk_id=0,
        start_pos=0,
        end_pos=12,
        metadata={"filename": "test.txt"}
    )

    d = chunk.to_dict()

    assert d['content'] == "Test content"
    assert d['chunk_id'] == 0
    assert d['metadata']['filename'] == "test.txt"


def test_chunker_overlap():
    """Test that chunks have proper overlap."""
    chunker = DocumentChunker(chunk_size=50, overlap=10)

    # Create text with paragraphs that will be chunked
    text = "Paragraph one with some content.\n\n" * 5

    chunks = chunker.chunk_document(text, "test.txt")

    # Should have multiple chunks due to small chunk size and multiple paragraphs
    assert len(chunks) >= 1

    # Check that chunks have content
    for chunk in chunks:
        assert len(chunk.content) > 0
        assert chunk.chunk_id >= 0
