import numpy as np
import boto3
from moto import mock_s3
import pytest
import os
from .S3_image_functions import S3Images
from PIL import Image
from PIL import ImageChops

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="ap-southeast-2")


def test_ImagesS3(s3):
    size = (200, 200)
    color = (255, 0, 0, 0)
    img = Image.new("RGB", size, color)

    images = S3Images()
    region = "ap-southeast-2"
    location = {'LocationConstraint': region}
    s3.create_bucket(Bucket="testbucket", CreateBucketConfiguration =location)

    images.to_s3(img= img,bucket = "testbucket", key = 'testkey.png')
    # compare downloaded image to original image as arrays (as comparing
    # images is quite difficult).
    dl_image =  images.from_s3('testbucket', 'testkey.png')

    assert (np.array(img) == np.array(dl_image)).all()



