from scripts.config import Config
from scripts.scraper import Scraper
from scripts.database import Database

# define python functions for airflow Operators

def scrape_file(**kwargs):
    '''
    Description here
    '''
    s = Scraper(kwargs['url'], Config)
    s.connect_s3_sink()
    s.url_to_s3(filename=kwargs['filename'],
                filters=kwargs['filters'],
                nullstr=kwargs['nullstr'])

def load_file(**kwargs):
    '''
    Description here
    table must be truncated
    '''
    db = Database(Config)
    db.connect()
    db.connect_s3_source()
    db.csv_to_table(filename=kwargs['filename'], 
                    table_name=kwargs['table_name'],
                    sep=kwargs['sep'],
                    nullstr=kwargs['nullstr'])