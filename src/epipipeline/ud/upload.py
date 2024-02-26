import boto3


def upload_files(Bucket: str, Key: str, Filename: str) -> bool:
    """_summary_

    Args:
        Bucket (str): _description_
        Key (str): _description_
        Filename (str): _description_

    Returns:
        bool: _description_
    """

    client = boto3.client('s3')

    client.upload_file(Filename=Filename, Bucket=Bucket, Key=Key)

    return True
