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


def build_content_blocks(
    title: str,
    body: str | None,
    comments: list[dict],
    created_utc: str | None = None,
    include_op: bool = True,
    max_blocks: int | None = None,
) -> list[dict]:
    """
    Converts a flat ordered list of comment dicts into the canonical ContentBlocks
    format consumed by LLM prompts.

    Args:
        title: Submission title (used in the OP block).
        body: Submission body text (used in the OP block).
        comments: Flat ordered list of comment dicts. Each must have 'body' and
            optionally 'author' and 'created_utc'. Deleted/removed comments are
            skipped automatically.
        created_utc: Timestamp for the OP block.
        include_op: Whether to prepend a block for the submission author (OP).
            Set to False for non-first chunks in a multi-chunk thread.
        max_blocks: If set, caps the total number of blocks returned.

    Returns:
        A list of ContentBlock dicts with block_id, author_id, text, created_utc.
    """
    blocks: list[dict] = []

    if include_op:
        blocks.append(
            {
                "block_id": 0,
                "author_id": "OP",
                "text": f"Title: {title}\nBody: {body or ''}",
                "created_utc": created_utc,
            }
        )

    for comment in comments:
        if not comment or not isinstance(comment, dict):
            continue
        text = comment.get("body", "")
        if not text or text in ("[deleted]", "[removed]"):
            continue
        block_id = len(blocks)
        blocks.append(
            {
                "block_id": block_id,
                "author_id": comment.get("author") or f"anon_{block_id}",
                "text": text,
                "created_utc": comment.get("created_utc"),
            }
        )
        if max_blocks and len(blocks) >= max_blocks:
            break

    return blocks


def build_mention_context(
    title: str,
    body: str | None,
    source_block_ids: list[int],
    content_blocks: list[dict],
) -> tuple[str, str]:
    """
    Resolves a discovered entity mention into the canonical (text, parent_text) pair
    used by downstream LLM phases (triage, extraction).

    This is the single source of truth for what those phases receive as context.
    Both the production unnesting asset and any eval or test that reconstructs
    mention inputs should call this — not assemble the strings inline.

    Args:
        title: Submission title.
        body: Submission body (selftext). May be None.
        source_block_ids: The block IDs flagged by discovery for this entity.
        content_blocks: The full list of ContentBlock dicts for the chunk.

    Returns:
        (text, parent_text) where:
          - text is the joined text of the source blocks, separated by "---"
          - parent_text is "Title: ...\nBody: ..." for use as submission context
    """
    matched_texts = [b["text"] for bid in source_block_ids for b in content_blocks if b.get("block_id") == bid]
    text = "\n\n---\n\n".join(matched_texts)
    parent_text = f"Title: {title}\nBody: {body or ''}"
    return text, parent_text


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
