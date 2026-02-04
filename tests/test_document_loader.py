"""
Tests for document loader.
"""
import pytest
import sys
from pathlib import Path
from io import BytesIO

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from common.document_loader import load_document, get_file_extension, _load_text


def test_get_file_extension():
    """Test file extension extraction."""
    assert get_file_extension("document.pdf") == "pdf"
    assert get_file_extension("document.docx") == "docx"
    assert get_file_extension("path/to/file.md") == "md"
    assert get_file_extension("file.txt") == "txt"


def test_load_text_file():
    """Test loading plain text files."""
    content = "This is a test document.\nWith multiple lines."
    file_obj = BytesIO(content.encode('utf-8'))

    text, images = load_document(file_obj, 'txt')

    assert text == content
    assert images == []


def test_load_markdown_file():
    """Test loading markdown files."""
    content = "# Heading\n\nThis is **bold** text."
    file_obj = BytesIO(content.encode('utf-8'))

    text, images = load_document(file_obj, 'md')

    assert text == content
    assert images == []


def test_unsupported_format():
    """Test that unsupported formats raise an error."""
    file_obj = BytesIO(b"content")

    with pytest.raises(ValueError, match="Unsupported file format"):
        load_document(file_obj, 'xyz')
