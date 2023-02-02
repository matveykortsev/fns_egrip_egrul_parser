import os
from os.path import basename


import logging

import boto3
import urllib3
from botocore.config import Config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class S3Connector:
    def __init__(self, s3_conn, proxy=""):
        self.url = s3_conn.host
        self.aws_key = s3_conn.login
        self.aws_secret = s3_conn.password
        self.proxy = proxy
        self.logger = logging.getLogger('temp_fns_egr')

    def connect(self):
        try:
            session = boto3.session.Session(
                aws_access_key_id=self.aws_key,
                aws_secret_access_key=self.aws_secret)
            s3 = session.client(service_name='s3',
                                endpoint_url=self.url,
                                config=Config(proxies={"http": self.proxy, "https": self.proxy}),
                                verify=False)
            self.logger.info(f"Connection to S3 Storage successfully established")
            return s3
        except Exception as e:
            self.logger.error(f"Unable to establish the connection with S3 Storage: {self.url}")
            raise e


def s3_upload(s3_client, bucket_dir, files_to_upload, bucket_name):
    logging.info(f'Found files in local directory to download: {files_to_upload}')
    for file in files_to_upload:
        bucket_path = f'{bucket_dir}/{os.path.basename(file)}'
        logging.info(f'Uploading to bucket {file} to {bucket_path}')
        try:
            s3_client.upload_file(file, bucket_name, bucket_path)
            logging.info(f'Successfully uploaded {file} to {bucket_path}')
            os.remove(file)
        except Exception as e:
            logging.warning(e)
            logging.error(f'Uploading failed. File {file} to /{bucket_path} was not uploaded')
            continue
