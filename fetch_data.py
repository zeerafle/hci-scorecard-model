import os
from dotenv import load_dotenv
import zipfile

load_dotenv()
DATA_DIR = 'dataset'
DATA_URL = os.getenv('DATA_URL')


def fetch_data(data_url: str):
    if os.path.exists('home-credit-default-risk.zip'):
        print('Data already exists')
    else:
        print('Fetching data...')
        os.system(f'wget {data_url}')
        print('Data fetched')

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print('Data directory created')

        with zipfile.ZipFile('home-credit-default-risk.zip', 'r') as zip_ref:
            zip_ref.extractall(DATA_DIR)
            print('Data extracted')
    else:
        print('Data already extracted')


fetch_data(DATA_URL)
