import boto3
import tempfile
import pandas as pd
import os


def download_file_from_URI(URI, extension):

    client = boto3.client('s3')

    URI = URI.removeprefix("s3://")
    Bucket = URI.split("/")[0]
    Key = URI.removeprefix(Bucket + "/")

    file = tempfile.NamedTemporaryFile(suffix="." + str(extension), delete=False)

    client.download_file(Bucket=Bucket, Key=Key,
                         Filename=file.name
                         )

    return file


def get_s3_versions_tags(Bucket: str, Prefix: str, Suffix: str, Contains: str):
    """
    Retrieves version information and associated tags for S3 objects matching specified criteria.

    Args:
        Bucket (str): The name of the S3 bucket.
        Prefix (str): The prefix to filter keys in the bucket.
        Suffix (str): The suffix to filter keys in the bucket.
        Contains (str): The substring to filter keys in the bucket.

    Returns:
        pandas.DataFrame: A DataFrame containing information about S3 object versions
        matching the specified criteria, including the key, version date, and version ID.

    Example:
        Suppose you have an S3 bucket 'my_bucket' containing various versions of Excel files.
        To retrieve version information and associated tags for all Excel files in the 'data/'
        folder with '2022' in their key names, you can use the function as follows:

        >>> import pandas as pd
        >>> from datetime import datetime
        >>> # Assuming boto3 and pandas are imported
        >>> versions_df = get_s3_versions_tags(Bucket='my_bucket', Prefix='data/', Suffix='.xlsx', Contains='2022')
        >>> print(versions_df)
                            Key          VersionDate           VersionId
        0  data/example.xlsx  2022-01-05T12:00:00Z  3Q4pDlMBg8wCVL7eE9Ekp3m0RTjmTIDF
        1  data/example.xlsx  2022-01-03T12:00:00Z  3Q4pDlMBg8wCVL7eE9Ekp3m0RTjmTIDF
        ...
    """
    client = boto3.client('s3')

    all_keys = client.list_objects_v2(
        Bucket=Bucket,
        Prefix=Prefix,
    )

    xlsx_keys = [content['Key'] for content in all_keys['Contents']
                 if content['Key'].endswith(Suffix)
                 if Contains in content['Key']
                 ]

    all_versions = client.list_object_versions(
        Bucket=Bucket,
        Prefix=Prefix,
    )

    version_dict = {VersionId['VersionId']: VersionId['Key']
                    for VersionId in all_versions['Versions']
                    if VersionId['Key'] in xlsx_keys}

    version_df = pd.DataFrame(columns=["Key", "VersionDate", "VersionId"])

    for VersionId, Key in version_dict.items():

        # Getting tags attached to specific key, versionId
        # and returning key of first tag, which is a date
        # print(Key)
        Tags = client.get_object_tagging(
            Bucket=Bucket,
            Key=Key,
            VersionId=VersionId
        )['TagSet'][0]['Key']

        version_df.loc[len(version_df.index)] = [Key, Tags, VersionId]

    version_df = version_df.sort_values(by=["Key", "Date"], ascending=[True, False])

    return version_df


def download_dataset(ds_info: dict,
                     custom_contains=False,
                     Contains=None,
                     latest=True,
                     VersionDate=None, VersionId=None,
                     check_for_expected_files=False,
                     expected_file_list=[None],
                     verbose=False
                     ):

    client = boto3.client('s3')

    # Check if Bucket, Prefix, Suffix, and Contains are in
    # ds_info dict

    Bucket = ds_info["Bucket"]
    Prefix = ds_info["Prefix"]
    Suffix = ds_info["Suffix"]

    if not custom_contains:
        Contains = ds_info["Contains"]

    if latest:
        client = boto3.client('s3')

        all_keys = client.list_objects_v2(
            Bucket=Bucket,
            Prefix=Prefix,
        )

        xlsx_keys = [content['Key'] for content in all_keys['Contents']
                     if content['Key'].endswith(Suffix)
                     if Contains in content['Key']
                     ]

        tmpdir = tempfile.TemporaryDirectory()

        for Key in xlsx_keys:
            client.download_file(Bucket=Bucket, Key=Key,
                                 Filename=tmpdir.name + "/" + Key.split("/")[-1])

    else:
        version_df = get_s3_versions_tags(Bucket=Bucket, Prefix=Prefix,
                                          Suffix=Suffix, Contains=Contains
                                          )

        tmpdir = tempfile.TemporaryDirectory()
        for Key in version_df['Key'].unique():

            VersionId = version_df[(version_df['Key'] == Key) &
                                   (version_df['VersionDate'] == VersionDate)].iloc[0]["VersionId"]

            client.download_file(Bucket=Bucket, Key=Key,
                                 Filename=tmpdir.name + "/" + Key.split("/")[-1],
                                 ExtraArgs={"VersionId": VersionId}
                                 )

    error = []
    if check_for_expected_files:
        if set(os.listdir(tmpdir.name)) == set(expected_file_list):
            error.append("All files found on bucket.")
        else:
            error.append("Required files not found on bucket. \nFiles required are: " +
                         str(expected_file_list) + ". \nFiles found are: " +
                         str(os.listdir(tmpdir.name))
                         )

    if verbose:
        for e in error:
            print(e)

    return tmpdir, error
