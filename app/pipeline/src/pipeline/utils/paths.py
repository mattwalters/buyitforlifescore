import os


def _get_local_base_dir() -> str:
    """Returns the local MacBook storage directory."""
    return "/Users/matt/src/mattwalters/buyitforlifescore/data"


def get_write_path(stage_and_filename: str) -> str:
    """
    Returns the target path to WRITE a file to.
    In production (DATA_DIR is set), writes to R2 bucket.
    Locally, writes to the MacBook drive without touching prod.

    Example usage:
        get_write_path("bronze/reddit_buyitforlife_comments.parquet")
    """
    data_dir = os.environ.get("DATA_DIR")
    if data_dir:
        # e.g., s3://buyitforlifescore/bronze/...
        return f"{str(data_dir).rstrip('/')}/{stage_and_filename}"

    local_dir = os.path.dirname(os.path.join(_get_local_base_dir(), stage_and_filename))
    os.makedirs(local_dir, exist_ok=True)
    return os.path.join(_get_local_base_dir(), stage_and_filename)


def get_read_path(stage_and_filename: str) -> str:
    """
    Returns the target path to READ a file from.
    Evaluates in Hybrid Mode:
        1. Always checks if the file physically exists on the local MacBook drive first.
        2. If missing, streams it from Prod/Cloudflare R2 seamlessly.
    """
    # 1. Local Override Detection (e.g., massive 'ore' ZST files downloaded via torrent)
    local_path = os.path.join(_get_local_base_dir(), stage_and_filename)
    if "*" in local_path:
        import glob

        if glob.glob(local_path):
            return local_path
    elif os.path.exists(local_path):
        return local_path

    # 2. Fallback to streaming from prod (Read-Only)
    data_dir = os.environ.get("DATA_DIR", "s3://buyitforlifescore")
    return f"{str(data_dir).rstrip('/')}/{stage_and_filename}"
