from dotenv import load_dotenv
import os

# Determine which environment we're in
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')  # default to 'development'

# Load the appropriate .env file
if ENVIRONMENT == 'production':
    env_path = os.path.join(os.path.dirname(__file__), '.env.prod')
    load_dotenv(env_path)
else:
    env_path = os.path.join(os.path.dirname(__file__), '.env.dev')
    load_dotenv(env_path)

# Access the environment variables
API_VERSION = '1.3.8'
JWT_SECRET=os.getenv('JWT_SECRET')
JWT_AUTH_HEADER_PREFIX=os.getenv('JWT_AUTH_HEADER_PREFIX')

ROOT_DOWNLOAD_FOLDER=os.getenv('ROOT_DOWNLOAD_FOLDER')

# DATABASE
PG_USER=os.getenv('PG_USER')
PG_PASSWORD=os.getenv('PG_PASSWORD')
PG_DATABASE=os.getenv('PG_DATABASE')
PG_HOST=os.getenv('PG_HOST')
PG_PORT=os.getenv('PG_PORT')

ENV=os.getenv('ENV')

SQLALCHEMY_DATABASE_URI=os.getenv('SQLALCHEMY_DATABASE_URI')

# SHAREPOINT
SHAREPOINT_KEY_PATH=os.getenv('SHAREPOINT_KEY_PATH')

# AWS S3
ACCESS_KEY=os.getenv('ACCESS_KEY')
SECRET_KEY=os.getenv('SECRET_KEY')
REGION_NAME=os.getenv('REGION_NAME')
BUCKET_NAME=os.getenv('BUCKET_NAME')

# AWS FILA SQS
SQS_PROCESSAMENTO_RENOVABIO=os.getenv('SQS_PROCESSAMENTO_RENOVABIO')
SQS_PROCESSAMENTO_RENOVABIO_DLQ=os.getenv('SQS_PROCESSAMENTO_RENOVABIO_DLQ')
SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER=os.getenv('SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER')
SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER_DLQ=os.getenv('SQS_PROCESSAMENTO_RENOVABIO_DISPATCHER_DLQ')
WAIT_TIME_SECONDS=os.getenv('WAIT_TIME_SECONDS')

# Sendgrid
SENDGRID_API_KEY=os.getenv('SENDGRID_API_KEY')

# FLASK
DEBUG=ENVIRONMENT == 'development'

EMAIL_FROM=os.getenv('EMAIL_FROM')
#EMAIL_TI=os.getenv('EMAIL_TI')

URL_PLATFORM=os.getenv('URL_PLATFORM')

# SINTEGRAWS INTEGRACAO
TOKEN_SINTEGRAWS=os.getenv('TOKEN_SINTEGRAWS')

#TRELLO
TRELLO_API_KEY=os.getenv('TRELLO_API_KEY')
TRELLO_API_SECRET=os.getenv('TRELLO_API_SECRET')
TRELLO_TOKEN=os.getenv('TRELLO_TOKEN')