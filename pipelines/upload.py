import os
import boto3
import pandas as pd
from utils.logger import setup_logger
logger = setup_logger()


class S3Uploader:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, bucket_name: str, region_name: str):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.bucket_name = bucket_name

    def check_file_exists(self, project_root_path: str, local_fullname: str):
        """Check if the file exists."""
        logger.info(f"current path is: {os.getcwd()}")
        if os.path.exists(f"{project_root_path}/{local_fullname}"):
            logger.info(f"The file {local_fullname} exists.")
            return True
        else:
            logger.info(f"The file {local_fullname} does not exist.")
            return False

    def upload_csv(self, sink_path: str, local_fullname: str = 'channel_reporting.csv', cloud_file_path: str = '', cloud_filename: str = 'channel_reporting'):
        try:
            if not self.check_file_exists(project_root_path=sink_path, local_fullname=local_fullname):
                raise FileNotFoundError()

            cloud_filename = f'{cloud_filename}_{
                pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv'
            target_filename = f"{cloud_file_path}/{cloud_filename}"
            logger.info(f"Uploading {local_fullname} to {
                        self.bucket_name}/{target_filename}")
            self.s3_client.upload_file(
                Filename=f"{sink_path}/{local_fullname}",  Bucket=self.bucket_name, Key=target_filename, )
            print(f"File {local_fullname} uploaded to {
                  self.bucket_name}/{target_filename}")
        except FileNotFoundError:
            print(f"The file {local_fullname} was not found")
        except Exception as e:
            print(f"An error occurred: {e}")


# Example usage:
# uploader = S3Uploader('your_access_key', 'your_secret_key', 'your_bucket_name')
# uploader.upload_csv('path/to/your/file.csv', 'your/s3/key.csv')
