import os

import sys
from dotenv import load_dotenv
import logging

import dags

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_LOCATION = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_LOCATION

AMBIENTE = os.getenv("FIEC_ANTAQ_ENV")

PATH_ARQUIVO_CONFIG = f"{ROOT_DIR}/.env.dev"

logging.basicConfig(format="[%(levelname)s] [%(asctime)s] %(message)s",
                    level=logging.INFO,
                    datefmt="%d/%m/%y %H:%M:%S",
                    encoding="utf-8")

if AMBIENTE is None:
    logging.info("Variável de ambiente FIEC_ANTAQ_ENV não existe.")
    AMBIENTE = 'DEV'

if AMBIENTE == 'PROD':
    PATH_ARQUIVO_CONFIG = f'{ROOT_DIR}/.env'

load_dotenv(PATH_ARQUIVO_CONFIG)

logging.info(f"Ambiente da aplicação:{AMBIENTE}")
logging.info(f"Python: {os.getenv('PYSPARK_PYTHON')}")


