import os
basedir = os.path.abspath(os.path.dirname(__file__))


# Configuration Variables
API_KEY = os.environ.get('API_KEY')
MONGO_DIALECT = os.environ.get('MONGO_DIALECT')
MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_PORT = os.environ.get('MONGO_PORT')
MONGO_USR = os.environ.get('MONGO_USR')
MONGO_PWD = os.environ.get('MONGO_PWD')
MONGO_DB = os.environ.get('MONGO_DB')
MONGO_URL = os.environ.get('MONGO_URL')
