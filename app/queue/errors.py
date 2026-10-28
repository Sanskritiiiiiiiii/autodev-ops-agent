class PermanentJobError(Exception):
    """Raised by a handler when retrying cannot help (e.g. the repo is not indexed)."""