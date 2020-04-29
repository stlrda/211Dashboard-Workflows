import sys
from loguru import logger
import requests
import boto3

class Scraper:
    """PostgreSQL Database class."""

    def __init__(self, url, config):
        self.url = url
        self.bucket_name = config.S3_BUCKET
        self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
        self.aws_region = config.AWS_REGION
        self.s3_conn = None
    
    def connect_s3_sink(self):
        if self.s3_conn is None:
            try:
                self.s3_conn = boto3.resource(
                    's3',
                    region_name=self.aws_region,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key
                )
            except ClientError as e:
                logger.error(e)
                sys.exit()
            finally:
                logger.info('Successfully established AWS s3 connection.')

    def url_to_s3(self, filename=None):
        if filename is None:
            filename = self.url.split('/')[-1]
        
        # download file from web
        r = requests.get(self.url)

        # create object
        obj = self.s3_conn.Object(self.bucket_name, filename)
        obj.put(Body=r.content)
        logger.info(f'{filename} uploaded to s3 bucket.')



    
