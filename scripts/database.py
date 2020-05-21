# code found here: https://hackersandslackers.com/psycopg2-postgres-python/
# https://www.mydatahack.com/how-to-bulk-load-data-into-postgresql-with-python/

import sys
import logging
import psycopg2
import boto3


class Database:
    """Project Database class."""

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
        #self.aws_region = config.AWS_REGION
        self.s3_conn = None

    def connect(self):
        """Connect to Postgres database."""
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(host=self.host,
                                             user=self.username,
                                             password=self.password,
                                             port=self.port,
                                             dbname=self.dbname)
            except psycopg2.DatabaseError as e:
                logging.error(e)
                sys.exit()
            finally:
                logging.info('Database connection opened successfully.')

    def connect_s3_source(self):
        """Connect to AWS s3 storage."""
        if self.s3_conn is None:
            try:
                self.s3_conn = boto3.resource(
                    's3',
                    #region_name=self.aws_region,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key
                )
            except ClientError as e:
                logging.error(e)
                sys.exit()
            finally:
                logging.info('Successfully established AWS s3 connection.')

    def __check_for_current(self, filename):
        file_split = filename.split('.')
        file_split[0] = file_split[0] + '_current'
        curr_filename = '.'.join(file_split)
        try:
            self.s3_conn.meta.client.head_object(
                Bucket=self.bucket_name,
                Key=curr_filename
            )
        except:
            curr_filename = filename
        return curr_filename

    def csv_to_table(self, filename, table_name, sep=',', nullstr='NaN'):
        """
        This method uploads csv to a target table.
        
        Parameters
        ----------
        filename : str 
            file name with extension
        table_name : str
            name of PostgreSQL table in DB (must be truncated)
        sep : str
            delimiter; default is comma
        nullstr : str
            string which DB should interpret as NULL values

        """
        
        try:
            filename = self.__check_for_current(filename)
            cur = self.conn.cursor()
            obj = self.s3_conn.Object(self.bucket_name, filename)
            body = obj.get()['Body']
            sql = f"copy {table_name} from STDIN CSV HEADER QUOTE '\"' DELIMITER AS '{sep}' NULL '{nullstr}'"
            cur.copy_expert(sql, body)
            cur.execute("commit;")
            logging.info(f"Loaded data into {table_name}")
        except Exception as e:
            logging.error(e)
            sys.exit()

    def close(self):
        """Close the database client connection."""
        if self.conn is None:
            logging.info('No connection to close.')
        else:
            self.conn.close()
            logging.info('Postgres connection closed.')