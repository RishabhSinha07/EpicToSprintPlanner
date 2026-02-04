"""
Unit tests for image extraction functionality.
"""
import pytest
import sys
import os
from pathlib import Path
from io import BytesIO

# Set AWS region before importing boto3
os.environ.setdefault('AWS_REGION', 'us-east-1')
os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-1')

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from common.document_loader import (
    load_document,
    process_image_for_bedrock,
    _extract_images_from_pdf
)
from common.chunker import Chunk


class TestImageExtraction:
    """Test image extraction and processing."""

    def test_chunk_with_images_field(self):
        """Test that Chunk dataclass includes images field."""
        chunk = Chunk(
            content="Test content",
            chunk_id=0,
            start_pos=0,
            end_pos=12,
            metadata={},
            images=[]
        )

        assert hasattr(chunk, 'images')
        assert chunk.images == []

        # Test to_dict includes images
        chunk_dict = chunk.to_dict()
        assert 'images' in chunk_dict
        assert chunk_dict['images'] == []

    def test_chunk_with_image_metadata(self):
        """Test Chunk with actual image metadata."""
        images = [
            {
                "image_id": "img_0",
                "s3_key": "chunks/job/images/img_0.jpg",
                "media_type": "image/jpeg",
                "page_number": 1
            }
        ]

        chunk = Chunk(
            content="Test content",
            chunk_id=0,
            start_pos=0,
            end_pos=12,
            metadata={},
            images=images
        )

        chunk_dict = chunk.to_dict()
        assert len(chunk_dict['images']) == 1
        assert chunk_dict['images'][0]['image_id'] == "img_0"

    def test_load_document_returns_tuple(self):
        """Test that load_document returns (text, images) tuple."""
        # Create a simple text file
        text_content = b"Hello, world!"
        file_obj = BytesIO(text_content)

        result = load_document(file_obj, 'txt')

        # Should return tuple of (text, images)
        assert isinstance(result, tuple)
        assert len(result) == 2

        text, images = result
        assert isinstance(text, str)
        assert isinstance(images, list)
        assert text == "Hello, world!"
        assert images == []  # Text files have no images

    def test_load_document_with_extract_images_false(self):
        """Test load_document with extract_images=False."""
        text_content = b"Test content"
        file_obj = BytesIO(text_content)

        text, images = load_document(file_obj, 'txt', extract_images=False)

        assert text == "Test content"
        assert images == []

    def test_image_processing_small_image(self):
        """Test that small images are returned unchanged."""
        # Create a small dummy image (1x1 pixel JPEG)
        small_jpeg = bytes([
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46,
            0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01,
            0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9
        ])

        processed_bytes, media_type = process_image_for_bedrock(small_jpeg, 'jpeg')

        assert media_type == "image/jpeg"
        assert len(processed_bytes) <= 3_750_000  # Within Bedrock limit

    def test_extract_page_numbers_from_content(self):
        """Test extracting page numbers from chunk content."""
        from lambdas.chunker.handler import extract_page_numbers_from_content

        content = """--- Page 1 ---
Some content here
--- Page 2 ---
More content
--- Page 3 ---
Even more"""

        page_numbers = extract_page_numbers_from_content(content)

        assert isinstance(page_numbers, set)
        assert 1 in page_numbers
        assert 2 in page_numbers
        assert 3 in page_numbers
        assert len(page_numbers) == 3

    def test_extract_page_numbers_no_pages(self):
        """Test extracting page numbers from content without page markers."""
        from lambdas.chunker.handler import extract_page_numbers_from_content

        content = "Just some regular content without page markers"

        page_numbers = extract_page_numbers_from_content(content)

        assert isinstance(page_numbers, set)
        assert len(page_numbers) == 0


class TestMultimodalContent:
    """Test multimodal content building for Bedrock."""

    def test_build_multimodal_content_text_only(self):
        """Test building content with text only."""
        from lambdas.story_generator.handler import build_multimodal_content

        content = build_multimodal_content("Test prompt", images=None)

        assert isinstance(content, list)
        assert len(content) == 1
        assert content[0]['type'] == 'text'
        assert content[0]['text'] == "Test prompt"

    def test_build_multimodal_content_with_images(self):
        """Test building content with images."""
        from lambdas.story_generator.handler import build_multimodal_content

        images = [
            {
                "data": "base64_encoded_image_data",
                "media_type": "image/jpeg"
            }
        ]

        content = build_multimodal_content("Test prompt", images=images)

        assert isinstance(content, list)
        assert len(content) == 2  # Image + text

        # First should be image
        assert content[0]['type'] == 'image'
        assert content[0]['source']['type'] == 'base64'
        assert content[0]['source']['media_type'] == 'image/jpeg'
        assert content[0]['source']['data'] == "base64_encoded_image_data"

        # Second should be text
        assert content[1]['type'] == 'text'
        assert content[1]['text'] == "Test prompt"

    def test_build_multimodal_content_multiple_images(self):
        """Test building content with multiple images."""
        from lambdas.story_generator.handler import build_multimodal_content

        images = [
            {"data": "img1_data", "media_type": "image/jpeg"},
            {"data": "img2_data", "media_type": "image/png"}
        ]

        content = build_multimodal_content("Test prompt", images=images)

        assert len(content) == 3  # 2 images + 1 text
        assert content[0]['type'] == 'image'
        assert content[1]['type'] == 'image'
        assert content[2]['type'] == 'text'


class TestImageAssignment:
    """Test image assignment to chunks."""

    def test_assign_images_to_chunks_by_page(self):
        """Test assigning images to chunks based on page numbers."""
        from lambdas.chunker.handler import assign_images_to_chunks

        chunks = [
            Chunk(
                content="--- Page 1 ---\nContent for page 1",
                chunk_id=0,
                start_pos=0,
                end_pos=20,
                metadata={},
                images=[]
            ),
            Chunk(
                content="--- Page 2 ---\nContent for page 2",
                chunk_id=1,
                start_pos=20,
                end_pos=40,
                metadata={},
                images=[]
            )
        ]

        image_metadata = [
            {
                "image_id": "img_0",
                "s3_key": "test/img_0.jpg",
                "media_type": "image/jpeg",
                "page_number": 1
            },
            {
                "image_id": "img_1",
                "s3_key": "test/img_1.jpg",
                "media_type": "image/jpeg",
                "page_number": 2
            }
        ]

        assign_images_to_chunks(chunks, image_metadata)

        # Chunk 0 should have image from page 1
        assert len(chunks[0].images) == 1
        assert chunks[0].images[0]['image_id'] == 'img_0'

        # Chunk 1 should have image from page 2
        assert len(chunks[1].images) == 1
        assert chunks[1].images[0]['image_id'] == 'img_1'

    def test_assign_images_without_page_numbers(self):
        """Test assigning images without page numbers (e.g., from DOCX)."""
        from lambdas.chunker.handler import assign_images_to_chunks

        chunks = [
            Chunk(
                content="First chunk content",
                chunk_id=0,
                start_pos=0,
                end_pos=20,
                metadata={},
                images=[]
            ),
            Chunk(
                content="Second chunk content",
                chunk_id=1,
                start_pos=20,
                end_pos=40,
                metadata={},
                images=[]
            )
        ]

        image_metadata = [
            {
                "image_id": "img_0",
                "s3_key": "test/img_0.jpg",
                "media_type": "image/jpeg",
                "page_number": None  # No page number (DOCX)
            }
        ]

        assign_images_to_chunks(chunks, image_metadata)

        # Image without page number should be assigned to first chunk
        assert len(chunks[0].images) == 1
        assert chunks[0].images[0]['image_id'] == 'img_0'

        # Second chunk should have no images
        assert len(chunks[1].images) == 0
