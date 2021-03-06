import boto3
from PIL import Image
from io import BytesIO
import os


class S3Images(object):
    """Useage:

    images = S3Images(aws_access_key_id='abc',
                      aws_secret_access_key='defg',
                      region_name='ap-southeast-2')
    im = images.from_s3('my-example-bucket', 'chext_x_ray.png')
    """

    def __init__(
        self, aws_access_key_id=None, aws_secret_access_key=None, region_name=None
    ):
        """All parameters can be passed as strings. If none are passed,
        then the parameters will be attempted to be accessed from the
        environmental variables (or revert to default)."""

        if aws_access_key_id is None:
            aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
        if aws_secret_access_key is None:
            aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        if region_name is None:
            region_name = "ap-southeast-2"

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def from_s3(self, bucket, key):
        file_byte_string = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return Image.open(BytesIO(file_byte_string))

    # additonal function brought in for testing.

    def to_s3(self, img, bucket, key):
        buffer = BytesIO()
        img.save(buffer, self.__get_safe_ext(key))
        buffer.seek(0)
        sent_data = self.s3.put_object(Bucket=bucket, Key=key, Body=buffer)
        if sent_data["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception("failed to upload")

# from https://gist.github.com/ghandic/a48f450f3c011f44d42eea16a0c7014d
    def __get_safe_ext(self, key):
        ext = os.path.splitext(key)[-1].strip('.').upper()
        if ext in ['JPG', 'JPEG']:
            return 'JPEG'
        elif ext in ['PNG']:
            return 'PNG'
        else:
            raise S3ImagesInvalidExtension('Extension is invalid')

class S3ImagesInvalidExtension(Exception):
    pass

class S3ImagesUploadFailed(Exception):
    pass