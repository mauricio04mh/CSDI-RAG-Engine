from __future__ import annotations

import pytest

from src.indexing.builder.segment_merge_policy import SegmentMergePolicy


def test_requires_at_least_two_max_segments():
    with pytest.raises(ValueError, match="at least 2"):
        SegmentMergePolicy(max_segments=1)


def test_no_candidates_when_under_limit():
    policy = SegmentMergePolicy(max_segments=3)
    assert policy.select_candidates(["s1", "s2", "s3"]) == []


def test_no_candidates_when_at_limit():
    policy = SegmentMergePolicy(max_segments=3)
    assert policy.select_candidates(["s1", "s2", "s3"]) == []


def test_returns_candidates_when_over_limit():
    policy = SegmentMergePolicy(max_segments=3)
    candidates = policy.select_candidates(["s1", "s2", "s3", "s4"])
    assert len(candidates) >= 2


def test_always_returns_at_least_two_candidates():
    policy = SegmentMergePolicy(max_segments=3)
    candidates = policy.select_candidates(["s1", "s2", "s3", "s4"])
    assert len(candidates) >= 2


def test_returns_oldest_segments():
    policy = SegmentMergePolicy(max_segments=3)
    # Alphabetically, s1 and s2 are the oldest
    candidates = policy.select_candidates(["s1", "s2", "s3", "s4"])
    assert candidates[0] == "s1"


def test_empty_list_returns_empty():
    policy = SegmentMergePolicy(max_segments=3)
    assert policy.select_candidates([]) == []


def test_single_segment_returns_empty():
    policy = SegmentMergePolicy(max_segments=3)
    assert policy.select_candidates(["s1"]) == []


def test_batch_size_correct():
    # max_segments=3, 5 segments → need to merge 3 oldest (5 - 3 + 1 = 3)
    policy = SegmentMergePolicy(max_segments=3)
    candidates = policy.select_candidates(["s1", "s2", "s3", "s4", "s5"])
    assert len(candidates) == 3
    assert candidates == ["s1", "s2", "s3"]
