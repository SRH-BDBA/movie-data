import os
basedir = os.path.abspath(os.path.dirname(__file__))


# Configuration Variables
API_KEY = "77ac0a1314a3ce073d073a61984c1205"
MONGO_DIALECT = os.environ.get('MONGO_DIALECT')
MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_PORT = os.environ.get('MONGO_PORT')
MONGO_USR = os.environ.get('MONGO_USR')
MONGO_PWD = os.environ.get('MONGO_PWD')
MONGO_DB = os.environ.get('MONGO_DB')
#MONGO_URL = os.environ.get('MONGO_URL')
#MONGO_URL = "mongodb+srv://mongo_admin:WbbqdlA0RexUWO1p@mongocluster.zkb97.mongodb.net/movies_1?retryWrites=true&w=majority"
MONGO_URL = 'mongodb+srv://data_pipeline:datapipeline1234@cluster0.mdwsf.mongodb.net/movies?retryWrites=true&w=majority'
