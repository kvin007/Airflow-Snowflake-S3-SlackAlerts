import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class AWSS3Helper():
    def __init__(self, aws_s3_conn_id: str):
        self.s3_hook = S3Hook(aws_conn_id=aws_s3_conn_id)


    def upload_file(self, local_file_path: str, destination_name: str, bucket_name: str, **kwargs):
        self.s3_hook.load_file(
            filename=local_file_path,
            key=destination_name,
            bucket_name=bucket_name,
            **kwargs
        )
        logging.info(f'File {local_file_path} was uploaded to {bucket_name}/{destination_name}') 