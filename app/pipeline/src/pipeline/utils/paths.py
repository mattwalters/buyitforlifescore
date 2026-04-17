import os


def get_write_path(stage_and_filename: str) -> str:
    """
    Returns the target path to WRITE a file to in R2.
    """
    data_dir = os.environ.get("DATA_DIR", "s3://buyitforlifescore")
    return f"{str(data_dir).rstrip('/')}/{stage_and_filename}"


def get_read_path(stage_and_filename: str) -> str:
    """
    Returns the target path to READ a file from in R2.
    """
    data_dir = os.environ.get("DATA_DIR", "s3://buyitforlifescore")
    return f"{str(data_dir).rstrip('/')}/{stage_and_filename}"
