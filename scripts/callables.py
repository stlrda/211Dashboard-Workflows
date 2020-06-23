from scripts.config import Config
from scripts.scraper import Scraper
from scripts.database import Database

# define python functions for airflow Operators

def scrape_file(**kwargs):
    '''Calls "url_to_s3" method of Scraper class'''
    s = Scraper(kwargs['url'], Config)
    s.connect_s3_sink()
    s.url_to_s3(filename=kwargs['filename'],
                filters=kwargs['filters'],
                nullstr=kwargs['nullstr'])


def scrape_api(**kwargs):
    '''Calls "api_to_s3" method of Scraper class.'''
    s = Scraper(kwargs['url'], Config)
    s.connect_s3_sink()
    s.api_to_s3(filename=kwargs['filename'],
                table_name=kwargs['table_name'], 
                limit=kwargs['limit'])


def scrape_transform(**kwargs):
    '''Calls "url_transform_to_s3" method of Scraper class'''
    s = Scraper(kwargs['url'], Config)
    s.connect_s3_sink()
    s.url_transform_to_s3(filename=kwargs['filename'],
                          transformer=kwargs['transformer'], 
                          sep=kwargs['sep'])


def transform_static_s3(**kwargs):
    '''
    Calls "s3_transform_to_s3" method of Scraper class
    '''
    s = Scraper(None, Config)  # None type url
    s.connect_s3_sink()
    s.s3_transform_to_s3(data=kwargs['data'],
                         output_filename=kwargs['filename'],
                         resource_path=kwargs['resource_path'],
                         transformer=kwargs['transformer'],
                         sep=kwargs['sep'])


def load_file(**kwargs):
    '''Calls "csv_to_table" method of Database class'''
    #NOTE table_name must be truncated before
    db = Database(Config)
    db.connect()
    db.connect_s3_source()
    db.csv_to_table(filename=kwargs['filename'], 
                    table_name=kwargs['table_name'],
                    sep=kwargs['sep'],
                    nullstr=kwargs['nullstr'])
    db.close()
