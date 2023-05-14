import psycopg2 as pg
import json
from configs.settings import settings

def create_psql_connection():
    with open('configs/config.json') as f:
        configs = json.load(f)
    settings.logger.info("Creating PSQL connection...")
    try:
        conn = pg.connect(
            host=configs.get("host"),
            database=configs.get("database"),
            user=configs.get("user"),
            password=configs.get("password"),
            port=configs.get("port")
        )
        settings.psql_conn = conn
        settings.logger.info("PSQL connection created!")
    except Exception as e:
        settings.logger.error(f"Error {e} connecting to PSQL server!!")