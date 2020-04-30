"""Load config from environment variables."""
from os import environ
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Project Configuration class."""
    # Database config
    DATABASE_HOST = environ.get('DATABASE_HOST')
    DATABASE_USERNAME = environ.get('DATABASE_USERNAME')
    DATABASE_PASSWORD = environ.get('DATABASE_PASSWORD')
    DATABASE_PORT = environ.get('DATABASE_PORT')
    DATABASE_NAME = environ.get('DATABASE_NAME')

    # SQL queries

    # AWS config
    S3_BUCKET = environ.get('S3_BUCKET')
    AWS_ACCESS_KEY_ID=environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY=environ.get('AWS_SECRET_ACCESS_KEY')
    AWS_REGION=environ.get('AWS_REGION')

    # MO Data API config
    API_TOKEN = environ.get('MO_API_TOKEN')
    API_USER_EMAIL = environ.get('MO_API_USER_EMAIL')
    API_USER_PWD = environ.get('MO_API_USER_PWD')
