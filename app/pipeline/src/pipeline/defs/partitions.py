from dagster import StaticPartitionsDefinition

"""
The definitive source of truth for supported subreddits.
MUST always be entirely lowercase to prevent cross-platform file system bugs and string matching errors.

To add a new subreddit:
1. Ensure the raw Reddit ZST dumps are placed in the `ore/` folder with lowercase names:
   e.g., `ore/reddit_frugal_comments.zst` and `ore/reddit_frugal_submissions.zst`
2. Add the lowercase subreddit name to the list below (e.g., `"frugal"`).
"""
subreddit_partitions = StaticPartitionsDefinition(["buyitforlife"])
