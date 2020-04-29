# code found here: https://hackersandslackers.com/psycopg2-postgres-python/
# https://www.mydatahack.com/how-to-bulk-load-data-into-postgresql-with-python/

import sys
from loguru import logger
import psycopg2
import boto3


class Database:
    """PostgreSQL Database class."""

    def __init__(self, config):
        self.host = config.DATABASE_HOST
        self.username = config.DATABASE_USERNAME
        self.password = config.DATABASE_PASSWORD
        self.port = config.DATABASE_PORT
        self.dbname = config.DATABASE_NAME
        self.conn = None
        self.bucket_name = config.S3_BUCKET
        self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
        self.aws_region = config.AWS_REGION
        self.s3_conn = None

    def connect(self):
        """Connect to a Postgres database."""
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(host=self.host,
                                             user=self.username,
                                             password=self.password,
                                             port=self.port,
                                             dbname=self.dbname)
            except psycopg2.DatabaseError as e:
                logger.error(e)
                sys.exit()
            finally:
                logger.info('Database connection opened successfully.')

    def connect_s3_source(self):
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

    def csv_to_table(self, filename, table_name, delimiter=','):
        '''
        This function uploads csv to a target table
        '''
        try:
            cur = self.conn.cursor()
            obj = self.s3_conn.Object(self.bucket_name, filename)
            body = obj.get()['Body']
            cur.copy_expert(f"copy {table_name} from STDIN CSV HEADER QUOTE '\"' DELIMITER AS '{delimiter}'", body)
            cur.execute("commit;")
            logger.info(f"Loaded data into {table_name}")
        except Exception as e:
            logger.error(e)
            sys.exit()