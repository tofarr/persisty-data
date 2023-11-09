import boto3

_s3_client = None


# pylint: disable=W0603
def get_s3_client():
    global _s3_client
    if not _s3_client:
        _s3_client = boto3.client("s3")
    return _s3_client
