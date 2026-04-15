"""
Tree utilities for reconstructing and chunking Reddit comment trees.

Reddit comments form a tree via the `parent_id` field:
  - Top-level comments have parent_id = "t3_<submission_id>"
  - Replies have parent_id = "t1_<parent_comment_id>"

This module rebuilds that tree in memory and walks it depth-first to produce
chunks that preserve conversational locality — complete branches are never
split across chunks.
"""

from collections import defaultdict


def build_comment_tree(comments: list[dict], submission_id: str) -> dict[str, list[dict]]:
    """
    Builds an adjacency list (parent -> children) from a flat list of comment dicts.

    Each comment dict must have 'id' and 'parent_id' keys.
    Top-level comments are those whose parent_id == "t3_<submission_id>".

    Returns:
        A dict mapping parent keys to ordered lists of child comment dicts.
        The special key "__roots__" contains the top-level comments.
    """
    children_of: dict[str, list[dict]] = defaultdict(list)
    top_level_parent = f"t3_{submission_id}"

    for comment in comments:
        if not comment or not isinstance(comment, dict):
            continue
        parent = comment.get("parent_id", "")
        if parent == top_level_parent:
            children_of["__roots__"].append(comment)
        else:
            children_of[parent].append(comment)

    return children_of


def _walk_branch(comment: dict, tree: dict[str, list[dict]]) -> list[dict]:
    """
    Depth-first walk starting from `comment`, returning a flat ordered list
    where every parent immediately precedes its children.
    """
    result = [comment]
    comment_key = f"t1_{comment.get('id', '')}"
    for child in tree.get(comment_key, []):
        result.extend(_walk_branch(child, tree))
    return result


def chunk_branches(tree: dict[str, list[dict]], max_chunk_size: int = 20) -> list[list[dict]]:
    """
    Walks the comment tree depth-first and packs complete conversational branches
    into chunks of approximately `max_chunk_size` comments.

    Rules:
      1. A branch is never split. If adding a branch would exceed max_chunk_size
         AND the current bucket is non-empty, the bucket is sealed first.
      2. A single branch that exceeds max_chunk_size is shipped as its own
         oversized chunk (we never cut a conversation mid-thread).
      3. Multiple small branches are packed together until the limit is reached.

    Args:
        tree: The adjacency list from build_comment_tree().
        max_chunk_size: Soft maximum number of comments per chunk.

    Returns:
        A list of chunks, where each chunk is a list of comment dicts in
        depth-first order with full branch locality preserved.
    """
    roots = tree.get("__roots__", [])
    chunks: list[list[dict]] = []
    current_bucket: list[dict] = []

    for root_comment in roots:
        branch = _walk_branch(root_comment, tree)

        # If adding this branch would overflow AND the bucket isn't empty, seal it.
        if current_bucket and (len(current_bucket) + len(branch) > max_chunk_size):
            chunks.append(current_bucket)
            current_bucket = []

        current_bucket.extend(branch)

    # Flush any remaining comments.
    if current_bucket:
        chunks.append(current_bucket)

    return chunks
