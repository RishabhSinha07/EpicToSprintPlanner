"""
Aggregator Lambda Handler
Merges and deduplicates user stories from multiple chunks, resolves dependencies,
and exports to various formats including Jira-compatible JSON.
"""
import json
import os
import boto3
from typing import List, Dict, Set
from collections import defaultdict
import sys
from pathlib import Path

# Add common modules to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from common.scalable_story_merger import ScalableStoryMerger

s3_client = boto3.client('s3')

OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET')


def lambda_handler(event, context):
    """
    Lambda handler for story aggregation.

    Event format:
    {
        "job_id": "job_id"
    }
    """
    print(f"Received event: {json.dumps(event)}")

    try:
        job_id = event['job_id']
        print(f"Aggregating stories for job: {job_id}")

        # Load all story files for this job
        stories = load_all_stories(job_id)
        print(f"Loaded {len(stories)} total stories")

        # Merge duplicate stories using scalable three-tier approach
        merger = ScalableStoryMerger()
        unique_stories = merger.merge_stories(stories)
        print(f"After scalable merge: {len(unique_stories)} unique stories")
        print(merger.generate_merge_report(len(stories), len(unique_stories)))

        # Resolve dependencies and add IDs
        processed_stories = process_stories(unique_stories)

        # Generate different export formats
        outputs = {
            'stories.json': json.dumps(processed_stories, indent=2),
            'jira_import.json': json.dumps(convert_to_jira_format(processed_stories), indent=2),
            'summary.txt': generate_summary(processed_stories)
        }

        # Store outputs in S3
        output_keys = []
        for filename, content in outputs.items():
            output_key = f"output/{job_id}/{filename}"
            s3_client.put_object(
                Bucket=OUTPUT_BUCKET,
                Key=output_key,
                Body=content,
                ContentType='application/json' if filename.endswith('.json') else 'text/plain'
            )
            output_keys.append(output_key)
            print(f"Stored output at s3://{OUTPUT_BUCKET}/{output_key}")

        # Update job metadata
        update_job_metadata(job_id, {
            'status': 'completed',
            'total_stories': len(processed_stories),
            'output_files': output_keys
        })

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Stories aggregated successfully',
                'job_id': job_id,
                'total_stories': len(processed_stories),
                'output_files': output_keys
            })
        }

    except Exception as e:
        print(f"Error aggregating stories: {str(e)}")
        import traceback
        traceback.print_exc()

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


def load_all_stories(job_id: str) -> List[Dict]:
    """Load all story files for a job from S3."""
    stories = []
    prefix = f"stories/{job_id}/"

    # List all story files
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=OUTPUT_BUCKET, Prefix=prefix):
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            key = obj['Key']
            if key.endswith('_stories.json'):
                # Load stories from this file
                response = s3_client.get_object(Bucket=OUTPUT_BUCKET, Key=key)
                chunk_stories = json.loads(response['Body'].read().decode('utf-8'))
                stories.extend(chunk_stories)

    return stories


def deduplicate_stories(stories: List[Dict]) -> List[Dict]:
    """
    Deduplicate stories based on title similarity and content.

    Uses a simple approach: exact title matches and high content similarity.
    """
    seen_titles = {}
    unique_stories = []

    for story in stories:
        title = story.get('title', '').lower().strip()

        # Check for exact title match
        if title in seen_titles:
            # Merge acceptance criteria if different
            existing = seen_titles[title]
            existing_ac = set(existing.get('acceptance_criteria', []))
            new_ac = set(story.get('acceptance_criteria', []))

            # Add any new acceptance criteria
            combined_ac = list(existing_ac | new_ac)
            existing['acceptance_criteria'] = combined_ac

            print(f"Merged duplicate story: {title}")
        else:
            seen_titles[title] = story
            unique_stories.append(story)

    return unique_stories


def process_stories(stories: List[Dict]) -> List[Dict]:
    """
    Process stories: add IDs, resolve dependencies, calculate metrics.
    """
    # Assign IDs
    for idx, story in enumerate(stories, 1):
        story['id'] = f"STORY-{idx:03d}"

    # Build title to ID mapping for dependency resolution
    title_to_id = {story.get('title', ''): story['id'] for story in stories}

    # Resolve dependencies
    for story in stories:
        deps = story.get('dependencies', [])
        if deps:
            resolved_deps = []
            for dep in deps:
                dep_id = title_to_id.get(dep, dep)
                resolved_deps.append(dep_id)
            story['dependency_ids'] = resolved_deps

    # Sort by dependencies (stories with no dependencies first)
    sorted_stories = topological_sort(stories)

    return sorted_stories


def topological_sort(stories: List[Dict]) -> List[Dict]:
    """
    Sort stories by dependencies using topological sort.
    Stories with no dependencies come first.
    """
    # Simple approach: move stories with dependencies to the end
    no_deps = []
    with_deps = []

    for story in stories:
        if story.get('dependency_ids'):
            with_deps.append(story)
        else:
            no_deps.append(story)

    return no_deps + with_deps


def convert_to_jira_format(stories: List[Dict]) -> Dict:
    """
    Convert stories to Jira import format.

    Jira CSV import format mapped to JSON.
    """
    jira_issues = []

    for story in stories:
        issue = {
            'Summary': story.get('title', ''),
            'Description': story.get('user_story', ''),
            'Issue Type': 'Story',
            'Story Points': story.get('story_points', ''),
            'Acceptance Criteria': '\n'.join([
                f"- {ac}" for ac in story.get('acceptance_criteria', [])
            ]),
            'Labels': story.get('labels', []),
            'Custom Fields': {
                'Technical Notes': story.get('technical_notes', ''),
                'Dependencies': story.get('dependency_ids', [])
            }
        }
        jira_issues.append(issue)

    return {
        'issues': jira_issues
    }


def generate_summary(stories: List[Dict]) -> str:
    """Generate a text summary of the stories."""
    lines = [
        "=" * 80,
        "User Stories Summary",
        "=" * 80,
        f"\nTotal Stories: {len(stories)}",
        f"Total Story Points: {sum(story.get('story_points', 0) for story in stories)}",
        "\n" + "=" * 80,
        "\nStories by Priority:\n"
    ]

    for story in stories:
        lines.append(f"\n{story.get('id', 'N/A')}: {story.get('title', 'Untitled')}")
        lines.append(f"  Points: {story.get('story_points', 'N/A')}")
        lines.append(f"  Story: {story.get('user_story', 'N/A')}")

        if story.get('acceptance_criteria'):
            lines.append("  Acceptance Criteria:")
            for ac in story['acceptance_criteria']:
                lines.append(f"    - {ac}")

        if story.get('dependency_ids'):
            lines.append(f"  Dependencies: {', '.join(story['dependency_ids'])}")

        lines.append("")

    return '\n'.join(lines)


def update_job_metadata(job_id: str, updates: Dict):
    """Update job metadata file with new information."""
    metadata_key = f"jobs/{job_id}/metadata.json"

    try:
        # Load existing metadata
        response = s3_client.get_object(Bucket=OUTPUT_BUCKET, Key=metadata_key)
        metadata = json.loads(response['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        metadata = {'job_id': job_id}

    # Update with new values
    metadata.update(updates)

    # Store back
    s3_client.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2),
        ContentType='application/json'
    )
