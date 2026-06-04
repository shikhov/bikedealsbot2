import os


DBNAME = 'bikedealsbot'
CONNSTRING = ''

WEBAPP_PATH = os.getenv('WEBAPP_PATH', '')
WEBAPP_HOST = os.getenv('WEBAPP_HOST', '')
WEBAPP_URL = f'https://{WEBAPP_HOST}/{WEBAPP_PATH}'
PORT = int(os.getenv('PORT', '8000'))
