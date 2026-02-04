#!/usr/bin/env python3
"""
Local E2E testing script for Epic to Sprint Planner.
Runs the full workflow: Loading -> Chunking -> Story Generation -> Aggregation
without requiring S3 or Lambda deployment.
"""
import os
import sys
import json
import argparse
import base64
from pathlib import Path
from dotenv import load_dotenv

# Add src to path so we can import our modules
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

def handle_absk_credentials():
    """
    Handle new AWS Bedrock API Keys (ABSK prefix).
    For Bedrock, these can be used as bearer tokens.
    """
    for env_var in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
        val = os.environ.get(env_var)
        if val and val.startswith('ABSK'):
            print(f"[*] Found ABSK Bedrock API Key in {env_var}.")
            # Set the bearer token env var which boto3's Bedrock client recommends
            os.environ['AWS_BEARER_TOKEN_BEDROCK'] = val
            
            # Clear standard keys to ensure boto3 uses the bearer token
            if 'AWS_ACCESS_KEY_ID' in os.environ: del os.environ['AWS_ACCESS_KEY_ID']
            if 'AWS_SECRET_ACCESS_KEY' in os.environ: del os.environ['AWS_SECRET_ACCESS_KEY']
            print("    Configured AWS_BEARER_TOKEN_BEDROCK and cleared standard credentials.")
            return

def normalize_story(story: dict) -> dict:
    """Normalize camelCase keys to snake_case."""
    mapping = {
        'userStory': 'user_story',
        'acceptanceCriteria': 'acceptance_criteria',
        'storyPoints': 'story_points',
        'technicalNotes': 'technical_notes'
    }
    new_story = story.copy()
    for camel, snake in mapping.items():
        if camel in new_story and snake not in new_story:
            new_story[snake] = new_story.pop(camel)
    return new_story

# Load environment variables from .env if it exists
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Handle ABSK keys before setting other defaults
handle_absk_credentials()

# Set dummy env vars for imports if not provided in .env
os.environ.setdefault('OUTPUT_BUCKET', 'local-test-bucket')
os.environ.setdefault('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20241022-v2:0')
region = os.environ.get('AWS_REGION', os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))
os.environ.setdefault('AWS_REGION', region)
os.environ.setdefault('AWS_DEFAULT_REGION', region)

from common.document_loader import load_document, get_file_extension
from common.chunker import DocumentChunker
from common.scalable_story_merger import ScalableStoryMerger
import lambdas.story_generator.handler as story_gen
import lambdas.aggregator.handler as aggregator

def main():
    parser = argparse.ArgumentParser(description="Run Epic to Sprint Planner locally.")
    parser.add_argument("input_file", help="Path to the input document (PDF, DOCX, MD, TXT)")
    parser.add_argument("--output-dir", default="_temp_output", help="Directory to save results")
    parser.add_argument("--chunk-size", type=int, default=4000, help="Max characters per chunk")
    parser.add_argument("--overlap", type=int, default=200, help="Character overlap between chunks")
    
    args = parser.parse_args()
    
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: File {args.input_file} not found.")
        sys.exit(1)
        
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Starting Local E2E Workflow")
    print(f"Input: {args.input_file}")
    print(f"Output Directory: {args.output_dir}")
    print(f"{'='*60}\n")
    
    # 1. Load Document
    print(f"[*] Loading document...")
    ext = get_file_extension(args.input_file)
    with open(args.input_file, 'rb') as f:
        text, images = load_document(f, ext, extract_images=True)
    print(f"    Loaded {len(text)} characters and {len(images)} images.")
    
    # 2. Chunk Document
    print(f"[*] Chunking document (size={args.chunk_size}, overlap={args.overlap})...")
    chunker = DocumentChunker(chunk_size=args.chunk_size, overlap=args.overlap)
    chunks = chunker.chunk_document(text, input_path.name)
    print(f"    Created {len(chunks)} chunks.")

    # 2.5. Distribute images to chunks using smart distribution
    if images:
        print(f"[*] Distributing {len(images)} images across {len(chunks)} chunks...")
        from lambdas.chunker.handler import assign_images_to_chunks

        # Create metadata for images (simulated, without S3 upload)
        image_metadata = []
        for img in images:
            image_metadata.append({
                "image_id": img['image_id'],
                "s3_key": f"local/images/{img['image_id']}.jpg",
                "media_type": img['media_type'],
                "page_number": img.get('page_number'),
                "image_index": img.get('image_index', 0),
                "total_images": img.get('total_images', len(images))
            })

        # Assign images to chunks
        assign_images_to_chunks(chunks, image_metadata)

        # Show distribution
        for chunk in chunks:
            if chunk.images:
                print(f"    Chunk {chunk.chunk_id}: {len(chunk.images)} images")
    
    # 3. Generate Stories
    print(f"[*] Generating stories using Bedrock ({os.environ['BEDROCK_MODEL_ID']})...")
    all_stories = []
    for i, chunk in enumerate(chunks):
        print(f"    Processing chunk {i+1}/{len(chunks)}...")
        try:
            # Use images assigned to this chunk by smart distribution
            chunk_images = []
            if hasattr(chunk, 'images') and chunk.images:
                # Convert images to the format expected by generate_stories
                # Need to get the actual image data from the original images list
                for img_meta in chunk.images:
                    # Find the corresponding image data
                    for img in images:
                        if img['image_id'] == img_meta['image_id']:
                            chunk_images.append({
                                "data": base64.b64encode(img['image_data']).decode('utf-8'),
                                "media_type": img['media_type']
                            })
                            break

                if chunk_images:
                    print(f"    Including {len(chunk_images)} images in chunk {i+1}")

            raw_stories = story_gen.generate_stories(chunk.content, chunk_images if chunk_images else None)
            # Normalize and validate
            stories = [normalize_story(s) for s in raw_stories]
            print(f"    Generated {len(stories)} stories.")
            # Add metadata similar to what the lambda does
            for story in stories:
                story['source_chunk_id'] = chunk.chunk_id
                story['job_id'] = 'local_job'
            all_stories.extend(stories)
        except Exception as e:
            print(f"    Error in chunk {i+1}: {e}")
            
    if not all_stories:
        print("Error: No stories generated. Exiting.")
        sys.exit(1)
        
    # 4. Merge and Process using Scalable Three-Tier Merger
    print(f"[*] Merging and deduplicating {len(all_stories)} stories using scalable merger...")
    merger = ScalableStoryMerger()
    unique_stories = merger.merge_stories(all_stories)
    print(f"    Found {len(unique_stories)} unique stories after scalable merge.")
    print(merger.generate_merge_report(len(all_stories), len(unique_stories)))

    processed_stories = aggregator.process_stories(unique_stories)
    
    # 5. Export results
    print(f"[*] Exporting results to {args.output_dir}...")
    
    stories_json = json.dumps(processed_stories, indent=2)
    with open(output_dir / 'stories.json', 'w') as f:
        f.write(stories_json)
        
    jira_json = json.dumps(aggregator.convert_to_jira_format(processed_stories), indent=2)
    with open(output_dir / 'jira_import.json', 'w') as f:
        f.write(jira_json)
        
    summary_text = aggregator.generate_summary(processed_stories)
    with open(output_dir / 'summary.txt', 'w') as f:
        f.write(summary_text)
        
    print(f"\n{'='*60}")
    print(f"Workflow Complete!")
    print(f"Files generated:")
    print(f"  - {output_dir / 'stories.json'}")
    print(f"  - {output_dir / 'jira_import.json'}")
    print(f"  - {output_dir / 'summary.txt'}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
