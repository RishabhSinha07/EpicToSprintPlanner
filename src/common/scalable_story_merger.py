"""
Scalable Story Merger

Three-tier approach for merging stories at scale:
1. Fast pre-filtering (title similarity, keywords)
2. LLM verification of candidates (lightweight)
3. LLM intelligent merge (comprehensive)

Scales from 10 to 10,000+ stories efficiently.
"""
import json
import os
import boto3
from typing import List, Dict, Tuple, Set
from difflib import SequenceMatcher
import re


class ScalableStoryMerger:
    """Scalable story merger using three-tier approach."""

    def __init__(self, bedrock_model_id: str = None):
        """
        Initialize scalable merger.

        Args:
            bedrock_model_id: Bedrock model ID (defaults to Claude 3.5 Sonnet)
        """
        self.bedrock_runtime = boto3.client('bedrock-runtime')
        self.model_id = bedrock_model_id or os.environ.get(
            'BEDROCK_MODEL_ID',
            'anthropic.claude-3-5-sonnet-20241022-v2:0'
        )

        # Tier 1 thresholds
        self.title_similarity_threshold = 0.75
        self.keyword_match_threshold = 2  # Min shared keywords

    def merge_stories(self, stories: List[Dict]) -> List[Dict]:
        """
        Merge duplicate stories using scalable three-tier approach.

        Args:
            stories: List of story dictionaries

        Returns:
            List of unique stories with duplicates merged
        """
        if not stories:
            return []

        if len(stories) == 1:
            return stories

        print(f"\n{'='*70}")
        print(f"Scalable Story Merger: Processing {len(stories)} stories")
        print(f"{'='*70}")

        # Tier 1: Fast pre-filtering
        candidate_pairs = self._tier1_fast_filtering(stories)
        print(f"Tier 1: Identified {len(candidate_pairs)} candidate duplicate pairs")

        if not candidate_pairs:
            print("No duplicate candidates found. Skipping LLM verification.")
            return stories

        # Tier 2: LLM verification (lightweight)
        confirmed_pairs = self._tier2_llm_verification(stories, candidate_pairs)
        print(f"Tier 2: Confirmed {len(confirmed_pairs)} duplicate pairs")

        if not confirmed_pairs:
            print("No duplicates confirmed. Returning original stories.")
            return stories

        # Tier 3: LLM intelligent merge
        merged_stories = self._tier3_llm_merge(stories, confirmed_pairs)

        duplicates_removed = len(stories) - len(merged_stories)
        print(f"Tier 3: Merged {duplicates_removed} duplicate stories")
        print(f"Final: {len(merged_stories)} unique stories")
        print(f"{'='*70}\n")

        return merged_stories

    def _tier1_fast_filtering(self, stories: List[Dict]) -> List[Tuple[int, int]]:
        """
        Tier 1: Fast pre-filtering using title similarity and keywords.

        Returns:
            List of (index1, index2) pairs that are candidate duplicates
        """
        candidates = []

        # Compare each pair of stories
        for i in range(len(stories)):
            for j in range(i + 1, len(stories)):
                if self._are_candidate_duplicates(stories[i], stories[j]):
                    candidates.append((i, j))

        return candidates

    def _are_candidate_duplicates(self, story1: Dict, story2: Dict) -> bool:
        """
        Quick check if two stories might be duplicates.

        Uses:
        1. Title similarity
        2. Core concept matching (keywords)
        """
        title1 = story1.get('title', '').lower().strip()
        title2 = story2.get('title', '').lower().strip()

        # Title similarity
        similarity = SequenceMatcher(None, title1, title2).ratio()
        if similarity >= self.title_similarity_threshold:
            return True

        # Core concept matching
        words1 = self._extract_keywords(title1)
        words2 = self._extract_keywords(title2)
        shared_keywords = words1 & words2

        if len(shared_keywords) >= self.keyword_match_threshold:
            return True

        return False

    def _extract_keywords(self, text: str) -> Set[str]:
        """Extract meaningful keywords from text."""
        # Extract words (3+ chars)
        words = set(re.findall(r'\b\w{3,}\b', text.lower()))

        # Remove common words
        stopwords = {
            'system', 'implementation', 'comprehensive', 'basic', 'simple',
            'advanced', 'complete', 'full', 'management', 'feature',
            'user', 'users', 'with', 'from', 'that', 'this', 'have', 'has'
        }
        return words - stopwords

    def _tier2_llm_verification(
        self,
        stories: List[Dict],
        candidate_pairs: List[Tuple[int, int]]
    ) -> List[Tuple[int, int]]:
        """
        Tier 2: Use LLM to verify which candidates are true duplicates.

        Sends lightweight payloads (title + user_story only) for verification.

        Returns:
            List of (index1, index2) pairs that are confirmed duplicates
        """
        if not candidate_pairs:
            return []

        # Prepare lightweight story representations
        lightweight_stories = []
        for i, story in enumerate(stories):
            lightweight_stories.append({
                'index': i,
                'title': story.get('title', ''),
                'user_story': story.get('user_story', '')[:200]  # First 200 chars only
            })

        # Prepare candidate pairs for LLM
        pairs_to_verify = []
        for idx1, idx2 in candidate_pairs:
            pairs_to_verify.append({
                'pair_id': f"{idx1}-{idx2}",
                'story1': lightweight_stories[idx1],
                'story2': lightweight_stories[idx2]
            })

        # Call LLM in batches
        confirmed = []
        batch_size = 50  # Verify up to 50 pairs at once

        for i in range(0, len(pairs_to_verify), batch_size):
            batch = pairs_to_verify[i:i + batch_size]
            batch_confirmed = self._call_llm_verification(batch)
            confirmed.extend(batch_confirmed)

        return confirmed

    def _call_llm_verification(self, pairs: List[Dict]) -> List[Tuple[int, int]]:
        """
        Call LLM to verify if candidate pairs are true duplicates.

        Args:
            pairs: List of pair dictionaries with lightweight story info

        Returns:
            List of (index1, index2) confirmed duplicate pairs
        """
        prompt = f"""Analyze these candidate duplicate pairs and determine which are TRUE duplicates:

<candidate_pairs>
{json.dumps(pairs, indent=2)}
</candidate_pairs>

For each pair, return true/false indicating if they describe the SAME feature (even if worded differently).

Examples:
- "Audit Logging System" + "Comprehensive Audit Logging System" → TRUE (same feature)
- "Email Registration" + "Email Verification" → FALSE (different features)
- "Google OAuth" + "Facebook Login" → FALSE (different providers)

Return JSON:
{{
  "confirmed_duplicates": [
    {{"pair_id": "12-19", "is_duplicate": true, "reason": "Both describe audit logging"}},
    {{"pair_id": "5-8", "is_duplicate": false, "reason": "Different aspects of profile"}}
  ]
}}

Return ONLY the JSON, no markdown."""

        try:
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "system": "You are an expert at identifying duplicate user stories. Be conservative - only mark as duplicates if they clearly describe the same feature.",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
            }

            response = self.bedrock_runtime.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body)
            )

            response_body = json.loads(response['body'].read())
            assistant_message = response_body['content'][0]['text']

            # Parse response
            result = self._parse_json_response(assistant_message)

            # Extract confirmed pairs
            confirmed = []
            for item in result.get('confirmed_duplicates', []):
                if item.get('is_duplicate', False):
                    pair_id = item['pair_id']
                    idx1, idx2 = map(int, pair_id.split('-'))
                    confirmed.append((idx1, idx2))
                    print(f"  ✓ Confirmed duplicate: {pair_id} - {item.get('reason', '')}")

            return confirmed

        except Exception as e:
            print(f"Warning: LLM verification failed: {e}")
            # Conservative fallback: assume all candidates are duplicates
            return [(int(p['pair_id'].split('-')[0]), int(p['pair_id'].split('-')[1])) for p in pairs]

    def _tier3_llm_merge(
        self,
        stories: List[Dict],
        confirmed_pairs: List[Tuple[int, int]]
    ) -> List[Dict]:
        """
        Tier 3: Use LLM to intelligently merge confirmed duplicate pairs.

        Sends full story objects only for stories that need merging.

        Args:
            stories: All stories
            confirmed_pairs: List of (index1, index2) confirmed duplicates

        Returns:
            List of merged stories
        """
        # Build merge groups (handle transitive duplicates)
        merge_groups = self._build_merge_groups(confirmed_pairs, len(stories))

        # For single-story groups (no duplicates), just keep original
        merged_stories = []
        stories_to_merge = []

        for group in merge_groups:
            if len(group) == 1:
                # No duplicates, keep original
                merged_stories.append(stories[group[0]])
            else:
                # Multiple stories to merge
                stories_to_merge.append(group)

        # Merge each group using LLM
        for group in stories_to_merge:
            group_stories = [stories[idx] for idx in group]
            merged = self._call_llm_merge(group_stories, group)
            merged_stories.append(merged)

            titles = [stories[idx]['title'] for idx in group]
            print(f"  ✓ Merged: {' + '.join(titles)}")
            print(f"    → {merged['title']}")

        return merged_stories

    def _build_merge_groups(
        self,
        pairs: List[Tuple[int, int]],
        total_stories: int
    ) -> List[List[int]]:
        """
        Build merge groups handling transitive duplicates.

        Example: If (1,2) and (2,3) are duplicates, merge all three: [1,2,3]

        Returns:
            List of groups, where each group is a list of story indices
        """
        # Union-find to group transitively connected stories
        parent = list(range(total_stories))

        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        # Union all pairs
        for idx1, idx2 in pairs:
            union(idx1, idx2)

        # Group by parent
        groups_dict = {}
        for i in range(total_stories):
            root = find(i)
            if root not in groups_dict:
                groups_dict[root] = []
            groups_dict[root].append(i)

        return list(groups_dict.values())

    def _call_llm_merge(self, group_stories: List[Dict], indices: List[int]) -> Dict:
        """
        Use LLM to merge a group of duplicate stories into one.

        Args:
            group_stories: List of full story objects to merge
            indices: Original indices of these stories

        Returns:
            Merged story dictionary
        """
        prompt = f"""Merge these duplicate stories into one comprehensive story:

<stories_to_merge>
{json.dumps(group_stories, indent=2)}
</stories_to_merge>

Merge strategy:
- Title: Choose the clearest, most concise title
- User Story: Keep the most detailed version
- Description: Combine both descriptions
- Acceptance Criteria: Union of all criteria (remove exact duplicates)
- Story Points: Take the highest estimate
- Dependencies: Union of all dependencies
- Technical Notes: Combine all notes

Return JSON:
{{
  "title": "...",
  "user_story": "...",
  "description": "...",
  "acceptance_criteria": [...],
  "story_points": 13,
  "dependencies": [...],
  "technical_notes": "..."
}}

Return ONLY the JSON."""

        try:
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "system": "You are an expert at merging duplicate user stories while preserving all important information.",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
            }

            response = self.bedrock_runtime.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body)
            )

            response_body = json.loads(response['body'].read())
            assistant_message = response_body['content'][0]['text']

            merged_story = self._parse_json_response(assistant_message)

            # Add metadata
            merged_story['merged'] = True
            merged_story['merged_from_indices'] = indices
            merged_story['merged_from_chunks'] = list(set(s.get('source_chunk_id') for s in group_stories if s.get('source_chunk_id') is not None))
            merged_story['job_id'] = group_stories[0].get('job_id')

            return merged_story

        except Exception as e:
            print(f"Warning: LLM merge failed: {e}. Using first story.")
            # Fallback: return first story
            fallback = group_stories[0].copy()
            fallback['merged'] = True
            fallback['merged_from_indices'] = indices
            return fallback

    def _parse_json_response(self, response: str) -> Dict:
        """Parse JSON from LLM response."""
        if '```json' in response:
            json_str = response.split('```json')[1].split('```')[0].strip()
        elif '```' in response:
            json_str = response.split('```')[1].split('```')[0].strip()
        else:
            json_str = response.strip()

        return json.loads(json_str)

    def generate_merge_report(self, original_count: int, merged_count: int) -> str:
        """Generate merge report."""
        duplicates_removed = original_count - merged_count
        reduction_pct = (duplicates_removed / original_count * 100) if original_count > 0 else 0

        return f"""
{'='*70}
Scalable Story Merge Report
{'='*70}

Original Stories:     {original_count}
After Merge:          {merged_count}
Duplicates Removed:   {duplicates_removed}
Reduction:            {reduction_pct:.1f}%

Method:               Three-tier scalable approach
Model:                {self.model_id}
Tier 1:               Fast pre-filtering (title + keywords)
Tier 2:               LLM verification (lightweight)
Tier 3:               LLM intelligent merge (comprehensive)

{'='*70}
"""
