"""
Chunker Lambda Handler
Splits uploaded documents into processable chunks and stores them in S3.
"""
import json
import os
import boto3
from io import BytesIO
import sys

# Add common modules to path
sys.path.insert(0, '/opt/python')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from common.document_loader import load_document, get_file_extension
from common.chunker import DocumentChunker

s3_client = boto3.client('s3')

# Configuration from environment variables
CHUNK_SIZE = int(os.environ.get('CHUNK_SIZE', 4000))
OVERLAP_SIZE = int(os.environ.get('OVERLAP_SIZE', 200))
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET')


def lambda_handler(event, context):
    """
    Lambda handler for document chunking.

    Event format (S3 trigger):
    {
        "Records": [{
            "s3": {
                "bucket": {"name": "bucket-name"},
                "object": {"key": "file.pdf"}
            }
        }]
    }

    Or for direct invocation:
    {
        "bucket": "bucket-name",
        "key": "file.pdf"
    }
    """
    print(f"Received event: {json.dumps(event)}")

    try:
        # Parse event
        if 'Records' in event:
            # S3 trigger event
            record = event['Records'][0]
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
        else:
            # Direct invocation
            bucket = event['bucket']
            key = event['key']

        print(f"Processing file: s3://{bucket}/{key}")

        # Download document from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_obj = BytesIO(response['Body'].read())

        # Extract file extension
        file_extension = get_file_extension(key)
        print(f"File extension: {file_extension}")

        # Load document content and images
        text, images = load_document(file_obj, file_extension, extract_images=True)
        print(f"Loaded document, length: {len(text)} characters, images: {len(images)}")

        # Chunk the document
        chunker = DocumentChunker(chunk_size=CHUNK_SIZE, overlap=OVERLAP_SIZE)
        chunks = chunker.chunk_document(text, filename=key)
        print(f"Created {len(chunks)} chunks")

        # Store images in S3 and get metadata
        job_id = key.replace('/', '_').replace('.', '_')
        if images:
            image_metadata = store_images(images, job_id)
            # Assign images to chunks based on page numbers
            assign_images_to_chunks(chunks, image_metadata)
            print(f"Stored {len(image_metadata)} images and assigned to chunks")

        # Store chunks in S3
        chunk_files = []

        for chunk in chunks:
            chunk_key = f"chunks/{job_id}/chunk_{chunk.chunk_id}.json"
            chunk_data = chunk.to_dict()
            chunk_data['job_id'] = job_id
            chunk_data['total_chunks'] = len(chunks)

            s3_client.put_object(
                Bucket=OUTPUT_BUCKET,
                Key=chunk_key,
                Body=json.dumps(chunk_data),
                ContentType='application/json'
            )
            chunk_files.append(chunk_key)
            print(f"Stored chunk {chunk.chunk_id} at s3://{OUTPUT_BUCKET}/{chunk_key}")

        # Store metadata about the chunking job
        metadata = {
            'job_id': job_id,
            'source_file': f"s3://{bucket}/{key}",
            'total_chunks': len(chunks),
            'chunk_files': chunk_files,
            'status': 'chunked'
        }

        metadata_key = f"jobs/{job_id}/metadata.json"
        s3_client.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=metadata_key,
            Body=json.dumps(metadata),
            ContentType='application/json'
        )

        print(f"Job metadata stored at s3://{OUTPUT_BUCKET}/{metadata_key}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Document chunked successfully',
                'job_id': job_id,
                'total_chunks': len(chunks),
                'metadata_key': metadata_key
            })
        }

    except Exception as e:
        print(f"Error processing document: {str(e)}")
        import traceback
        traceback.print_exc()

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


def store_images(images: list, job_id: str) -> list:
    """
    Store images in S3 and return metadata.

    Args:
        images: List of image dictionaries with image_data
        job_id: Job ID for organizing files

    Returns:
        List of image metadata dictionaries
    """
    image_metadata = []

    for img in images:
        try:
            image_id = img['image_id']
            image_data = img['image_data']
            media_type = img['media_type']
            page_number = img.get('page_number')
            image_index = img.get('image_index', 0)  # Order index for DOCX
            total_images = img.get('total_images', 1)

            # Determine file extension from media type
            ext = media_type.split('/')[-1]
            if ext == 'jpeg':
                ext = 'jpg'

            # Store processed image in S3
            image_key = f"chunks/{job_id}/images/processed/{image_id}.{ext}"
            s3_client.put_object(
                Bucket=OUTPUT_BUCKET,
                Key=image_key,
                Body=image_data,
                ContentType=media_type
            )

            metadata = {
                "image_id": image_id,
                "s3_key": image_key,
                "media_type": media_type,
                "page_number": page_number,
                "image_index": image_index,  # For sequential distribution
                "total_images": total_images
            }
            image_metadata.append(metadata)

            if page_number:
                print(f"Stored image {image_id} at s3://{OUTPUT_BUCKET}/{image_key} (page: {page_number})")
            else:
                print(f"Stored image {image_id} at s3://{OUTPUT_BUCKET}/{image_key} (index: {image_index}/{total_images})")

        except Exception as e:
            print(f"Warning: Failed to store image {img.get('image_id', 'unknown')}: {str(e)}")
            continue

    return image_metadata


def assign_images_to_chunks(chunks: list, image_metadata: list):
    """
    Assign images to chunks based on page numbers (PDF) or sequential distribution (DOCX).

    For PDFs: Images are assigned based on page number matching.
    For DOCX: Images are distributed evenly across chunks based on their order.

    Args:
        chunks: List of Chunk objects
        image_metadata: List of image metadata dictionaries
    """
    # Separate images by type
    pdf_images = [img for img in image_metadata if img.get('page_number') is not None]
    docx_images = [img for img in image_metadata if img.get('page_number') is None]

    # Assign PDF images based on page numbers
    for chunk in chunks:
        chunk_pages = extract_page_numbers_from_content(chunk.content)
        chunk_images = []

        # Add PDF images that match page numbers
        for img_meta in pdf_images:
            page_num = img_meta.get('page_number')
            if page_num in chunk_pages:
                chunk_images.append(img_meta)

        if chunk_images:
            chunk.images = chunk_images
            print(f"Assigned {len(chunk_images)} PDF images to chunk {chunk.chunk_id}")

    # Assign DOCX images using smart sequential distribution
    if docx_images and chunks:
        num_chunks = len(chunks)
        num_images = len(docx_images)

        # Strategy depends on image-to-chunk ratio
        if num_images <= num_chunks:
            # Fewer images than chunks: Distribute evenly
            # Image 0 -> Chunk 0, Image 1 -> Chunk 1, etc.
            for img_meta in docx_images:
                img_index = img_meta.get('image_index', 0)
                # Assign to corresponding chunk (with wraparound if needed)
                target_chunk_id = img_index % num_chunks

                for chunk in chunks:
                    if chunk.chunk_id == target_chunk_id:
                        if not chunk.images:
                            chunk.images = []
                        chunk.images.append(img_meta)
                        print(f"Assigned DOCX image {img_meta['image_id']} to chunk {chunk.chunk_id} (sequential distribution)")
                        break
        else:
            # More images than chunks: Distribute proportionally
            # Calculate how many images per chunk
            images_per_chunk = (num_images + num_chunks - 1) // num_chunks  # Ceiling division

            for chunk_idx, chunk in enumerate(chunks):
                # Calculate which images belong to this chunk
                start_img_idx = chunk_idx * images_per_chunk
                end_img_idx = min(start_img_idx + images_per_chunk, num_images)

                chunk_docx_images = [
                    img for img in docx_images
                    if start_img_idx <= img.get('image_index', 0) < end_img_idx
                ]

                if chunk_docx_images:
                    if not chunk.images:
                        chunk.images = []
                    chunk.images.extend(chunk_docx_images)
                    print(f"Assigned {len(chunk_docx_images)} DOCX images to chunk {chunk.chunk_id} (proportional distribution)")


def extract_page_numbers_from_content(content: str) -> set:
    """
    Extract page numbers from content by finding page markers.

    Args:
        content: Chunk text content

    Returns:
        Set of page numbers found in content
    """
    import re

    page_numbers = set()

    # Match "--- Page N ---" markers
    pattern = r'---\s*Page\s+(\d+)\s*---'
    matches = re.findall(pattern, content)

    for match in matches:
        page_numbers.add(int(match))

    return page_numbers
