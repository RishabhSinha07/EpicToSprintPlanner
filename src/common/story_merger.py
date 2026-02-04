"""
Story Merger Module

Intelligently merges duplicate user stories from different chunks using:
1. Semantic similarity detection (title and content)
2. Acceptance criteria overlap analysis
3. Intelligent field merging (combine best aspects of both stories)
"""
from typing import List, Dict, Tuple, Optional
from difflib import SequenceMatcher
import re


class StoryMerger:
    """Merges duplicate stories intelligently."""

    def __init__(
        self,
        title_similarity_threshold: float = 0.85,
        criteria_overlap_threshold: float = 0.5,
        fuzzy_title_threshold: float = 0.70
    ):
        """
        Initialize merger with configurable thresholds.

        Args:
            title_similarity_threshold: Exact title match threshold (0.0-1.0)
            criteria_overlap_threshold: Acceptance criteria overlap threshold
            fuzzy_title_threshold: Fuzzy title match threshold (when combined with criteria)
        """
        self.title_similarity_threshold = title_similarity_threshold
        self.criteria_overlap_threshold = criteria_overlap_threshold
        self.fuzzy_title_threshold = fuzzy_title_threshold

    def merge_stories(self, stories: List[Dict]) -> List[Dict]:
        """
        Merge duplicate stories from multiple chunks.

        Args:
            stories: List of story dictionaries from all chunks

        Returns:
            List of unique stories with duplicates merged
        """
        if not stories:
            return []

        unique_stories = []
        merge_log = []

        for story in stories:
            merged = False

            # Check against all existing unique stories
            for existing in unique_stories:
                if self._are_duplicates(story, existing):
                    # Merge the new story into the existing one
                    self._merge_into_existing(existing, story)
                    merge_log.append({
                        'merged': story.get('title'),
                        'into': existing.get('title'),
                        'reason': self._get_merge_reason(story, existing)
                    })
                    merged = True
                    break

            if not merged:
                unique_stories.append(story)

        # Log merge statistics
        if merge_log:
            print(f"\n{'='*70}")
            print(f"Story Merger: Merged {len(merge_log)} duplicate stories")
            print(f"{'='*70}")
            for log in merge_log:
                print(f"✓ Merged: '{log['merged']}'")
                print(f"  Into:   '{log['into']}'")
                print(f"  Reason: {log['reason']}")
            print(f"{'='*70}\n")

        return unique_stories

    def _are_duplicates(self, story1: Dict, story2: Dict) -> bool:
        """
        Determine if two stories are duplicates.

        Uses multiple heuristics:
        1. High title similarity (>85%)
        2. Moderate title similarity (>70%) + high criteria overlap (>50%)
        3. Exact keyword match in core concept
        """
        title1 = story1.get('title', '').lower().strip()
        title2 = story2.get('title', '').lower().strip()

        # Calculate title similarity
        title_sim = self._calculate_similarity(title1, title2)

        # High title similarity = duplicate
        if title_sim >= self.title_similarity_threshold:
            return True

        # Moderate title + criteria overlap
        if title_sim >= self.fuzzy_title_threshold:
            criteria_overlap = self._calculate_criteria_overlap(
                story1.get('acceptance_criteria', []),
                story2.get('acceptance_criteria', [])
            )
            if criteria_overlap >= self.criteria_overlap_threshold:
                return True

        # Check for exact keyword matches (e.g., both about "audit logging")
        if self._have_matching_core_concept(title1, title2):
            criteria_overlap = self._calculate_criteria_overlap(
                story1.get('acceptance_criteria', []),
                story2.get('acceptance_criteria', [])
            )
            if criteria_overlap >= 0.3:  # Lower threshold for concept matches
                return True

        return False

    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity ratio between two strings (0.0 to 1.0)."""
        return SequenceMatcher(None, text1, text2).ratio()

    def _calculate_criteria_overlap(
        self,
        criteria1: List[str],
        criteria2: List[str]
    ) -> float:
        """
        Calculate overlap between two sets of acceptance criteria.

        Uses fuzzy matching to detect semantically similar criteria.

        Returns:
            Fuzzy Jaccard similarity (0.0 to 1.0)
        """
        if not criteria1 or not criteria2:
            return 0.0

        # Normalize criteria
        norm1 = [self._normalize_criterion(c) for c in criteria1]
        norm2 = [self._normalize_criterion(c) for c in criteria2]

        # Use fuzzy matching to find matches
        matched_pairs = 0
        used_indices = set()

        for c1 in norm1:
            best_match_score = 0
            best_match_idx = -1

            for idx, c2 in enumerate(norm2):
                if idx in used_indices:
                    continue

                # Calculate similarity between criteria
                similarity = self._calculate_similarity(c1, c2)

                # Also check for key concept overlap
                concept_match = self._criteria_have_matching_concepts(c1, c2)

                # Consider it a match if high similarity OR concept match
                if similarity > 0.75 or concept_match:
                    if similarity > best_match_score:
                        best_match_score = similarity
                        best_match_idx = idx

            # If we found a good match, count it
            if best_match_score > 0.75 or (best_match_idx >= 0 and best_match_score > 0.6):
                matched_pairs += 1
                if best_match_idx >= 0:
                    used_indices.add(best_match_idx)

        # Calculate fuzzy Jaccard: matches / total unique criteria
        total_criteria = len(criteria1) + len(criteria2) - matched_pairs
        return matched_pairs / total_criteria if total_criteria > 0 else 0.0

    def _normalize_criterion(self, criterion: str) -> str:
        """Normalize a single criterion for comparison."""
        # Remove common prefixes/bullet points
        normalized = re.sub(r'^[-•*\s]+', '', criterion)
        # Lowercase and strip
        normalized = normalized.lower().strip()
        return normalized

    def _criteria_have_matching_concepts(self, criterion1: str, criterion2: str) -> bool:
        """
        Check if two criteria describe the same concept.

        Examples that should match:
        - "Log all user data access (read/write)" vs "Log all user data access (read/write/delete)"
        - "Retain logs for 7 years" vs "Log retention for 7 years"
        - "Admin interface for log review" vs "Log search and export capability"
        """
        # Extract key words (3+ chars, excluding common words)
        words1 = set(re.findall(r'\b\w{3,}\b', criterion1.lower()))
        words2 = set(re.findall(r'\b\w{3,}\b', criterion2.lower()))

        # Remove very common words
        common_words = {'all', 'the', 'and', 'for', 'with', 'from', 'that', 'this', 'are', 'have'}
        words1 -= common_words
        words2 -= common_words

        if not words1 or not words2:
            return False

        # Calculate word overlap
        overlap = len(words1 & words2)
        min_words = min(len(words1), len(words2))

        # If >60% of words overlap, consider it a concept match
        return overlap / min_words >= 0.6 if min_words > 0 else False

    def _have_matching_core_concept(self, title1: str, title2: str) -> bool:
        """
        Check if two titles share the same core concept.

        Examples:
        - "Audit Logging" and "Comprehensive Audit Logging"
        - "User Registration" and "Email Registration"
        """
        # Extract key terms (3+ character words)
        words1 = {w for w in re.findall(r'\b\w{3,}\b', title1.lower())}
        words2 = {w for w in re.findall(r'\b\w{3,}\b', title2.lower())}

        # Remove common generic words
        generic_words = {
            'system', 'implementation', 'comprehensive', 'basic', 'simple',
            'advanced', 'complete', 'full', 'management', 'feature'
        }
        words1 -= generic_words
        words2 -= generic_words

        # Check for significant word overlap (at least 2 shared important words)
        shared_words = words1 & words2
        return len(shared_words) >= 2

    def _merge_into_existing(self, existing: Dict, new: Dict) -> None:
        """
        Merge new story into existing story, combining best aspects.

        Merging strategy:
        - Title: Keep shorter, clearer title
        - User Story: Keep more detailed version
        - Description: Combine both
        - Acceptance Criteria: Union of both sets (deduplicated)
        - Story Points: Take higher estimate (conservative)
        - Dependencies: Union of dependencies
        - Technical Notes: Combine both
        """
        # 1. Title: Keep shorter or more specific
        if len(new.get('title', '')) < len(existing.get('title', '')):
            # Shorter title is often clearer
            if not self._is_too_generic(new.get('title', '')):
                existing['title'] = new['title']

        # 2. User Story: Keep more detailed (longer)
        existing_story = existing.get('user_story', '')
        new_story = new.get('user_story', '')
        if len(new_story) > len(existing_story):
            existing['user_story'] = new_story

        # 3. Description: Combine both (if different)
        existing_desc = existing.get('description', '')
        new_desc = new.get('description', '')
        if new_desc and new_desc not in existing_desc:
            existing['description'] = self._combine_descriptions(
                existing_desc,
                new_desc
            )

        # 4. Acceptance Criteria: Union (deduplicated)
        existing['acceptance_criteria'] = self._merge_acceptance_criteria(
            existing.get('acceptance_criteria', []),
            new.get('acceptance_criteria', [])
        )

        # 5. Story Points: Take higher (conservative estimate)
        existing['story_points'] = max(
            existing.get('story_points', 0),
            new.get('story_points', 0)
        )

        # 6. Dependencies: Union
        existing_deps = set(existing.get('dependencies', []))
        new_deps = set(new.get('dependencies', []))
        combined_deps = existing_deps | new_deps
        if combined_deps:
            existing['dependencies'] = sorted(list(combined_deps))

        # 7. Technical Notes: Combine
        existing['technical_notes'] = self._combine_technical_notes(
            existing.get('technical_notes', ''),
            new.get('technical_notes', '')
        )

        # 8. Track source chunks
        existing_chunks = existing.get('source_chunk_ids', [])
        if not existing_chunks:
            existing_chunks = [existing.get('source_chunk_id')]

        new_chunk = new.get('source_chunk_id')
        if new_chunk is not None and new_chunk not in existing_chunks:
            existing_chunks.append(new_chunk)

        existing['source_chunk_ids'] = existing_chunks
        existing['merged_from_chunks'] = len(existing_chunks)

    def _is_too_generic(self, title: str) -> bool:
        """Check if a title is too generic (e.g., just 'System' or 'Feature')."""
        generic_only = {'system', 'feature', 'implementation', 'management'}
        words = set(re.findall(r'\b\w+\b', title.lower()))
        return words.issubset(generic_only)

    def _combine_descriptions(self, desc1: str, desc2: str) -> str:
        """Combine two descriptions intelligently."""
        if not desc1:
            return desc2
        if not desc2:
            return desc1

        # If one contains the other, use the longer one
        if desc2 in desc1:
            return desc1
        if desc1 in desc2:
            return desc2

        # Otherwise, combine with a separator
        return f"{desc1}\n\nAdditional context: {desc2}"

    def _merge_acceptance_criteria(
        self,
        criteria1: List[str],
        criteria2: List[str]
    ) -> List[str]:
        """
        Merge two lists of acceptance criteria, removing duplicates.

        Uses fuzzy matching to detect similar criteria.
        """
        merged = list(criteria1)  # Start with first list

        for criterion in criteria2:
            # Check if this criterion is already covered
            is_duplicate = False
            for existing in merged:
                similarity = self._calculate_similarity(
                    self._normalize_criterion(criterion),
                    self._normalize_criterion(existing)
                )
                if similarity > 0.85:  # Very similar criteria
                    # Keep the more detailed one
                    if len(criterion) > len(existing):
                        merged[merged.index(existing)] = criterion
                    is_duplicate = True
                    break

            if not is_duplicate:
                merged.append(criterion)

        return merged

    def _combine_technical_notes(self, notes1: str, notes2: str) -> str:
        """Combine technical notes from both stories."""
        if not notes1:
            return notes2
        if not notes2:
            return notes1

        # If one contains the other, use the longer one
        if notes2 in notes1:
            return notes1
        if notes1 in notes2:
            return notes2

        # Combine with bullet points for clarity
        return f"{notes1}\n\nAdditional notes:\n{notes2}"

    def _get_merge_reason(self, story1: Dict, story2: Dict) -> str:
        """Get human-readable reason for merging."""
        title1 = story1.get('title', '').lower()
        title2 = story2.get('title', '').lower()

        title_sim = self._calculate_similarity(title1, title2)
        criteria_overlap = self._calculate_criteria_overlap(
            story1.get('acceptance_criteria', []),
            story2.get('acceptance_criteria', [])
        )

        if title_sim >= self.title_similarity_threshold:
            return f"High title similarity ({title_sim:.0%})"
        elif self._have_matching_core_concept(title1, title2):
            return f"Same core concept + criteria overlap ({criteria_overlap:.0%})"
        else:
            return f"Title similarity ({title_sim:.0%}) + criteria overlap ({criteria_overlap:.0%})"

    def generate_merge_report(self, original_count: int, merged_count: int) -> str:
        """Generate a summary report of the merge operation."""
        duplicates_removed = original_count - merged_count
        reduction_pct = (duplicates_removed / original_count * 100) if original_count > 0 else 0

        report = f"""
{'='*70}
Story Merge Report
{'='*70}

Original Stories:     {original_count}
After Merge:          {merged_count}
Duplicates Removed:   {duplicates_removed}
Reduction:            {reduction_pct:.1f}%

Configuration:
- Title similarity threshold:     {self.title_similarity_threshold:.0%}
- Criteria overlap threshold:     {self.criteria_overlap_threshold:.0%}
- Fuzzy title threshold:          {self.fuzzy_title_threshold:.0%}

{'='*70}
"""
        return report
