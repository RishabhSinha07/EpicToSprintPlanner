"""
LLM-Based Story Merger

Uses Claude (via Bedrock) to intelligently identify and merge duplicate stories.
This approach is more accurate than heuristic-based matching as the LLM can:
- Understand semantic similarity
- Identify duplicate concepts even with different wording
- Intelligently combine acceptance criteria
- Preserve all important information
"""
import json
import os
import boto3
from typing import List, Dict


class LLMStoryMerger:
    """Merges duplicate stories using LLM intelligence."""

    def __init__(self, bedrock_model_id: str = None):
        """
        Initialize LLM merger.

        Args:
            bedrock_model_id: Bedrock model ID to use (defaults to Claude 3.5 Sonnet)
        """
        self.bedrock_runtime = boto3.client('bedrock-runtime')
        self.model_id = bedrock_model_id or os.environ.get(
            'BEDROCK_MODEL_ID',
            'anthropic.claude-3-5-sonnet-20241022-v2:0'
        )

    def merge_stories(self, stories: List[Dict]) -> List[Dict]:
        """
        Merge duplicate stories using LLM analysis.

        Args:
            stories: List of story dictionaries from all chunks

        Returns:
            List of unique stories with duplicates intelligently merged
        """
        if not stories:
            return []

        if len(stories) == 1:
            return stories

        print(f"\n{'='*70}")
        print(f"LLM Story Merger: Analyzing {len(stories)} stories for duplicates...")
        print(f"{'='*70}")

        # Use LLM to identify and merge duplicates
        merged_stories = self._llm_merge_batch(stories)

        duplicates_removed = len(stories) - len(merged_stories)
        if duplicates_removed > 0:
            print(f"✓ Merged {duplicates_removed} duplicate stories")
            print(f"✓ Final count: {len(merged_stories)} unique stories")
        else:
            print(f"✓ No duplicates found")

        print(f"{'='*70}\n")

        return merged_stories

    def _llm_merge_batch(self, stories: List[Dict]) -> List[Dict]:
        """
        Use LLM to merge a batch of stories.

        For large batches (>30 stories), processes in smaller groups for better accuracy.
        """
        # If small batch, process all at once
        if len(stories) <= 30:
            return self._call_llm_merger(stories)

        # For larger batches, use two-pass approach:
        # Pass 1: Process in groups of 30
        # Pass 2: Merge results from pass 1
        print(f"Large batch detected. Using two-pass merge strategy...")

        pass1_results = []
        batch_size = 30

        for i in range(0, len(stories), batch_size):
            batch = stories[i:i + batch_size]
            print(f"  Pass 1: Processing batch {i//batch_size + 1} ({len(batch)} stories)")
            merged_batch = self._call_llm_merger(batch)
            pass1_results.extend(merged_batch)

        # If pass 1 reduced the count significantly, do a second pass
        if len(pass1_results) < len(stories) * 0.8 and len(pass1_results) > 30:
            print(f"  Pass 2: Final merge of {len(pass1_results)} stories")
            return self._call_llm_merger(pass1_results)

        return pass1_results

    def _call_llm_merger(self, stories: List[Dict]) -> List[Dict]:
        """
        Call LLM to identify and merge duplicate stories.

        Args:
            stories: List of stories to analyze and merge

        Returns:
            List of merged stories
        """
        # Prepare stories for LLM (simplified format)
        stories_for_llm = []
        for idx, story in enumerate(stories):
            stories_for_llm.append({
                'index': idx,
                'title': story.get('title', ''),
                'user_story': story.get('user_story', ''),
                'description': story.get('description', ''),
                'acceptance_criteria': story.get('acceptance_criteria', []),
                'story_points': story.get('story_points', 0),
                'dependencies': story.get('dependencies', []),
                'technical_notes': story.get('technical_notes', ''),
                'source_chunk_id': story.get('source_chunk_id')
            })

        prompt = self._build_merge_prompt(stories_for_llm)

        # Call Bedrock
        try:
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 16000,
                "system": self._get_system_prompt(),
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": 0.1,  # Low temperature for consistent merging
            }

            response = self.bedrock_runtime.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body)
            )

            response_body = json.loads(response['body'].read())
            assistant_message = response_body['content'][0]['text']

            # Parse JSON response
            merged_indices = self._parse_llm_response(assistant_message)

            # Reconstruct merged stories
            return self._reconstruct_stories(stories, merged_indices)

        except Exception as e:
            print(f"Error calling LLM for merge: {str(e)}")
            print("Falling back to original stories (no merge)")
            return stories

    def _get_system_prompt(self) -> str:
        """Get system prompt for merge task."""
        return """You are an expert at analyzing user stories and identifying duplicates.

Your task is to identify duplicate or highly similar user stories from different document chunks and merge them intelligently.

## What Makes Stories Duplicates?

Stories are duplicates if they describe the same feature/capability, even if worded differently:
- "Audit Logging System" and "Comprehensive Audit Logging System" → DUPLICATE
- "Email Registration" and "User Registration via Email" → DUPLICATE
- "Google OAuth" and "Google OAuth Integration" → DUPLICATE
- "Basic Profile Management" and "Profile Information Management" → DUPLICATE

Stories are NOT duplicates if they're different aspects of the same area:
- "Email Registration" and "Email Verification" → DIFFERENT (registration vs verification)
- "Google OAuth" and "Facebook OAuth" → DIFFERENT (different providers)
- "Basic Profile" and "Address Management" → DIFFERENT (different profile aspects)

## Merging Strategy

When merging duplicate stories:
1. **Title**: Choose the clearer, more concise title (avoid "Comprehensive", "Basic", etc.)
2. **User Story**: Keep the more detailed version
3. **Description**: Combine both descriptions
4. **Acceptance Criteria**: Union of both sets (remove exact duplicates)
5. **Story Points**: Take the higher estimate (conservative)
6. **Dependencies**: Union of dependencies
7. **Technical Notes**: Combine both
8. **Track Source**: Note that story was merged from multiple chunks

## Output Format

Return a JSON object with this structure:
{
  "merged_groups": [
    {
      "primary_index": 0,
      "merged_with_indices": [5, 12],
      "reason": "All three describe audit logging system",
      "merged_story": {
        "title": "Audit Logging System",
        "user_story": "...",
        "description": "...",
        "acceptance_criteria": [...],
        "story_points": 13,
        "dependencies": [...],
        "technical_notes": "...",
        "merged_from_chunks": [0, 1]
      }
    }
  ],
  "unique_indices": [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13]
}

Where:
- `merged_groups`: Groups of stories that were merged together
- `unique_indices`: Indices of stories that had no duplicates

CRITICAL: Every story index (0 to N-1) must appear exactly once, either in a merged group or in unique_indices."""

    def _build_merge_prompt(self, stories: List[Dict]) -> str:
        """Build the merge prompt with story data."""
        stories_json = json.dumps(stories, indent=2)

        return f"""Analyze the following user stories and identify duplicates to merge:

<stories>
{stories_json}
</stories>

Instructions:
1. Identify which stories are duplicates (same feature, even if worded differently)
2. Group duplicate stories together
3. For each group, merge them into a single comprehensive story
4. Return the merged stories and the list of unique story indices

Remember:
- Be conservative: only merge if stories are clearly about the same feature
- Preserve all important information when merging
- Combine acceptance criteria (remove exact duplicates)
- Take the higher story point estimate when merging

Return ONLY valid JSON with the structure specified in the system prompt."""

    def _parse_llm_response(self, response: str) -> Dict:
        """Parse LLM JSON response."""
        # Handle markdown code blocks
        if '```json' in response:
            json_str = response.split('```json')[1].split('```')[0].strip()
        elif '```' in response:
            json_str = response.split('```')[1].split('```')[0].strip()
        else:
            json_str = response.strip()

        return json.loads(json_str)

    def _reconstruct_stories(
        self,
        original_stories: List[Dict],
        merge_result: Dict
    ) -> List[Dict]:
        """
        Reconstruct the final story list from merge results.

        Args:
            original_stories: Original list of stories
            merge_result: LLM merge result with merged_groups and unique_indices

        Returns:
            Final merged story list
        """
        final_stories = []

        # Add merged stories
        for group in merge_result.get('merged_groups', []):
            merged_story = group.get('merged_story', {})

            # Get metadata from primary story
            primary_idx = group.get('primary_index', 0)
            primary_story = original_stories[primary_idx]

            # Combine with LLM-generated merged story
            final_story = {
                **merged_story,
                'job_id': primary_story.get('job_id'),
                'merged': True,
                'merged_from_indices': [primary_idx] + group.get('merged_with_indices', []),
                'merge_reason': group.get('reason', 'Duplicate stories')
            }

            final_stories.append(final_story)

            # Log the merge
            merged_titles = [original_stories[i].get('title', '') for i in final_story['merged_from_indices']]
            print(f"  ✓ Merged: {' + '.join(merged_titles)}")
            print(f"    → {final_story['title']}")

        # Add unique stories (no duplicates found)
        for idx in merge_result.get('unique_indices', []):
            if idx < len(original_stories):
                story = original_stories[idx].copy()
                story['merged'] = False
                final_stories.append(story)

        return final_stories

    def generate_merge_report(self, original_count: int, merged_count: int) -> str:
        """Generate a summary report of the merge operation."""
        duplicates_removed = original_count - merged_count
        reduction_pct = (duplicates_removed / original_count * 100) if original_count > 0 else 0

        report = f"""
{'='*70}
LLM Story Merge Report
{'='*70}

Original Stories:     {original_count}
After Merge:          {merged_count}
Duplicates Removed:   {duplicates_removed}
Reduction:            {reduction_pct:.1f}%

Method:               LLM-based semantic analysis
Model:                {self.model_id}

{'='*70}
"""
        return report
