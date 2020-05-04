"""Load config from environment variables."""
#from os import environ
#from dotenv import load_dotenv
from airflow.hooks.base_hook import BaseHook
import json

#load_dotenv()

class Config:
    """Project Configuration class."""
    # Database config
    DATABASE_CONN = BaseHook.get_connection('postgres')
    DATABASE_HOST = DATABASE_CONN.host
    DATABASE_USERNAME = DATABASE_CONN.login
    DATABASE_PASSWORD = DATABASE_CONN.password
    DATABASE_PORT = DATABASE_CONN.port
    DATABASE_NAME = DATABASE_CONN.conn_id

    # AWS S3 config
    S3_CONN = BaseHook.get_connection('s3-bucket')
    S3_BUCKET = S3_CONN.conn_id
    AWS_ACCESS_KEY_ID = json.loads(S3_CONN.extra)['aws_access_key_id']
    AWS_SECRET_ACCESS_KEY = json.loads(S3_CONN.extra)['aws_secret_access_key']

    # MO Data API config
    MO_API_CONN = BaseHook.get_connection('mo-api')
    API_TOKEN = json.loads(MO_API_CONN.extra)['token']
    API_HOST = MO_API_CONN.host
    API_USER_EMAIL = MO_API_CONN.login
    API_USER_PWD = MO_API_CONN.password
