"""Load config from environment variables."""
from os import environ
from dotenv import load_dotenv

load_dotenv()

class Config:
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