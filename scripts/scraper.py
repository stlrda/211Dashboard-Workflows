import sys
from loguru import logger
import requests
import boto3
import pandas as pd
from io import StringIO
import csv

class Scraper:
    """ Web scraping and file writing class. """

    def __init__(self, url, config):
        self.url = url
        self.bucket_name = config.S3_BUCKET
        self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
        #self.aws_region = config.AWS_REGION
        self.s3_conn = None  # aws s3 connection
        self.api_token = config.API_TOKEN
        self.api_user_email = config.API_USER_EMAIL
        self.api_user_pwd = config.API_USER_PWD

    def connect_s3_sink(self):
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
                logger.error(e)
                sys.exit()
            finally:
                logger.info('Successfully established AWS s3 connection.')

    def __rename_file(self, filename):
        """ Add '_current' to filename. """
        file_split = filename.split('.')
        file_split[0] = file_split[0]+'_current'
        return '.'.join(file_split)
    
    def __archive_file(self, curr_filename):
        """ 
        Archives 'current' file data in S3.

        Parameters
        ----------
        curr_filename : str 
            file name + '_current' + extension
        
        """
        archived_filename = curr_filename.replace('_current', '_previous')
        try:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': curr_filename
            }
            self.s3_conn.meta.client.copy(copy_source, self.bucket_name, archived_filename)
        except ClientError as e:
            logger.error(e)

    def url_to_s3(self, filename, filters=None, nullstr=''):
        """
        Upload file from web to s3 storage.

        Parameters
        ----------
        filename : str 
            file name with extension
        filters : dict
            key : str
                name of column to filter
            value: list
                list of accepted values for column
        nullstr : str
            used only when filtering
            string assigned to null values when writing csv file

        """
        
        if filters is None:
            # download file from web
            content = requests.get(self.url).content
        else:
            # load file from web to pandas df
            df = pd.read_csv(self.url)
            # filter df
            for key, values in filters.items():
                logger.info(f'Filtering "{key}" column...')
                df = df[df[key].isin(values)]
            # write df to csv
            csv_buf = StringIO()
            df.to_csv(csv_buf, header=True, index=False, na_rep=nullstr)
            csv_buf.seek(0)
            content = csv_buf.getvalue()
        
        # add "_current" to filename
        filename = self.__rename_file(filename)
        # copy "_current" file in bucket to "_previous"
        self.__archive_file(filename)
        # create s3 object
        obj = self.s3_conn.Object(self.bucket_name, filename)
        obj.put(Body=content)
        logger.info(f'{filename} uploaded to s3 bucket.')

    def api_to_s3(self, filename, table_name, limit=2000):
        """
        Loads data via api, uploads data file to s3.

        Parameters
        ----------
        filename : str 
            file name with extension
        table_name : str
            name of dataset fetched from api
        limit : int
            number of records to download

        """
        # load api module
        from sodapy import Socrata
        api_client = Socrata(self.url,
                             self.api_token,
                             self.api_user_email,
                             self.api_user_pwd)
        # get records
        logger.info(f'Fetching data from {self.url} API.')
        records = api_client.get(table_name, limit=limit)
        df = pd.DataFrame.from_records(records)
        
        # write csv
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        content = csv_buf.getvalue()

        # add "_current" to filename
        filename = self.__rename_file(filename)
        # copy "_current" file in bucket to "_previous"
        self.__archive_file(filename)
        # create s3 object
        obj = self.s3_conn.Object(self.bucket_name, filename)
        obj.put(Body=content)
        logger.info(f'{filename} uploaded to s3 bucket.')

    def url_transform_to_s3(self, filename, transform_func, sep='|'):
        """
        Description

        """
        df = transform_func(self.url)
        csv_buf = StringIO()
        df.to_csv(csv_buf, sep=sep, header=True, index=False)
        csv_buf.seek(0)
        content = csv_buf.getvalue()
        
        # add "_current" to filename
        filename = self.__rename_file(filename)
        # copy "_current" file in bucket to "_previous"
        self.__archive_file(filename)
        # create s3 object
        obj = self.s3_conn.Object(self.bucket_name, filename)
        obj.put(Body=content)
        logger.info(f'{filename} uploaded to s3 bucket.')