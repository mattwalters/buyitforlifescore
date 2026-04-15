"""Tests for pipeline.utils.tree — comment tree building and branch-aware chunking."""

import pytest
from pipeline.utils.tree import build_comment_tree, chunk_branches


# ---- Helpers ----

def _make_comment(comment_id: str, parent_id: str, body: str = "test") -> dict:
    return {
        "id": comment_id,
        "parent_id": parent_id,
        "body": body,
        "created_utc": "2024-01-01",
    }


SUBMISSION_ID = "abc123"
TOP_LEVEL_PARENT = f"t3_{SUBMISSION_ID}"


# ---- build_comment_tree ----

class TestBuildCommentTree:
    def test_empty_comments(self):
        tree = build_comment_tree([], SUBMISSION_ID)
        assert tree.get("__roots__", []) == []

    def test_all_top_level(self):
        comments = [
            _make_comment("c1", TOP_LEVEL_PARENT, "first"),
            _make_comment("c2", TOP_LEVEL_PARENT, "second"),
            _make_comment("c3", TOP_LEVEL_PARENT, "third"),
        ]
        tree = build_comment_tree(comments, SUBMISSION_ID)
        assert len(tree["__roots__"]) == 3

    def test_nested_replies(self):
        comments = [
            _make_comment("c1", TOP_LEVEL_PARENT, "top"),
            _make_comment("c2", "t1_c1", "reply to c1"),
            _make_comment("c3", "t1_c2", "reply to c2"),
        ]
        tree = build_comment_tree(comments, SUBMISSION_ID)
        assert len(tree["__roots__"]) == 1
        assert len(tree["t1_c1"]) == 1
        assert len(tree["t1_c2"]) == 1

    def test_skips_none_and_invalid(self):
        comments = [None, "not a dict", {}, _make_comment("c1", TOP_LEVEL_PARENT)]
        tree = build_comment_tree(comments, SUBMISSION_ID)
        # None and string are skipped; empty dict has no parent_id so goes to ""
        assert len(tree["__roots__"]) == 1


# ---- chunk_branches ----

class TestChunkBranches:
    def test_single_flat_branch(self):
        """A single top-level comment with no replies → one chunk."""
        comments = [_make_comment("c1", TOP_LEVEL_PARENT)]
        tree = build_comment_tree(comments, SUBMISSION_ID)
        chunks = chunk_branches(tree, max_chunk_size=20)
        assert len(chunks) == 1
        assert len(chunks[0]) == 1

    def test_multiple_small_branches_packed_together(self):
        """3 small branches (1 comment each) should pack into 1 chunk with max=20."""
        comments = [
            _make_comment("c1", TOP_LEVEL_PARENT),
            _make_comment("c2", TOP_LEVEL_PARENT),
            _make_comment("c3", TOP_LEVEL_PARENT),
        ]
        tree = build_comment_tree(comments, SUBMISSION_ID)
        chunks = chunk_branches(tree, max_chunk_size=20)
        assert len(chunks) == 1
        assert len(chunks[0]) == 3

    def test_branches_split_at_boundary(self):
        """Two branches of 3 each, with max_chunk_size=5 → should pack into 1 chunk (3+3=6 > 5, so 2 chunks)."""
        comments = [
            _make_comment("c1", TOP_LEVEL_PARENT),
            _make_comment("c1a", "t1_c1"),
            _make_comment("c1b", "t1_c1a"),
            _make_comment("c2", TOP_LEVEL_PARENT),
            _make_comment("c2a", "t1_c2"),
            _make_comment("c2b", "t1_c2a"),
        ]
        tree = build_comment_tree(comments, SUBMISSION_ID)
        chunks = chunk_branches(tree, max_chunk_size=5)
        assert len(chunks) == 2
        # First chunk: branch 1 (c1 → c1a → c1b)
        assert len(chunks[0]) == 3
        # Second chunk: branch 2 (c2 → c2a → c2b)
        assert len(chunks[1]) == 3

    def test_oversized_branch_never_split(self):
        """A single branch of 5 comments with max_chunk_size=3 ships as 1 chunk."""
        comments = [
            _make_comment("c1", TOP_LEVEL_PARENT),
            _make_comment("c2", "t1_c1"),
            _make_comment("c3", "t1_c2"),
            _make_comment("c4", "t1_c3"),
            _make_comment("c5", "t1_c4"),
        ]
        tree = build_comment_tree(comments, SUBMISSION_ID)
        chunks = chunk_branches(tree, max_chunk_size=3)
        assert len(chunks) == 1
        assert len(chunks[0]) == 5

    def test_depth_first_ordering(self):
        """Parent always precedes child in the chunk output."""
        comments = [
            _make_comment("c1", TOP_LEVEL_PARENT),
            _make_comment("c1a", "t1_c1"),
            _make_comment("c1b", "t1_c1a"),
            _make_comment("c1c", "t1_c1"),  # sibling of c1a
        ]
        tree = build_comment_tree(comments, SUBMISSION_ID)
        chunks = chunk_branches(tree, max_chunk_size=20)
        assert len(chunks) == 1
        ids = [c["id"] for c in chunks[0]]
        # c1 must come first, c1a before c1b (its child), c1c is sibling of c1a
        assert ids.index("c1") < ids.index("c1a")
        assert ids.index("c1a") < ids.index("c1b")
        assert ids.index("c1") < ids.index("c1c")

    def test_mixed_branches_packing(self):
        """Small branches pack together, large ones seal the bucket."""
        comments = [
            # Branch 1: single comment
            _make_comment("c1", TOP_LEVEL_PARENT),
            # Branch 2: 3 deep
            _make_comment("c2", TOP_LEVEL_PARENT),
            _make_comment("c2a", "t1_c2"),
            _make_comment("c2b", "t1_c2a"),
            # Branch 3: single comment
            _make_comment("c3", TOP_LEVEL_PARENT),
        ]
        tree = build_comment_tree(comments, SUBMISSION_ID)
        chunks = chunk_branches(tree, max_chunk_size=4)
        # Bucket starts with c1 (size 1), then c2 branch (size 3) → total 4 ≤ 4, fits.
        # Then c3 would make it 5 > 4, so bucket seals. c3 goes into new chunk.
        assert len(chunks) == 2
        assert len(chunks[0]) == 4  # c1 + c2 branch
        assert len(chunks[1]) == 1  # c3
