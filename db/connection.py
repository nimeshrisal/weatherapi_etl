import psycopg2
import logging
from psycopg2.extras import RealDictCursor
from config.config import Config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
class DatabaseConnection:
    def __init__(self, config: Config):
        self.db_params = config.get_db_params() 
        self.conn = None
        self.cursor = None


    def __enter__(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Database connection established successfully.")
            return self
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise

    def __exit__(self, exc_type, exc_value, traceback):

        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                if exc_type is None:
                    self.conn.commit()
                else:
                    self.conn.rollback()
                self.conn.close()
        except Exception as e:
            logger.error(f"Error closing the database connection: {e}")
            raise

    def execute(self, query, params=None):
        try:
            logger.info(f"Executing query: {query} with params: {params}")
            self.cursor.execute(query, params)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def fetchall(self):
        try:
            logger.info("Fetching all records from the database.")
            return self.cursor.fetchall()
        except Exception as e:
            logger.info(f"Error fetching data: {e}")
            raise
    
    def fetchone(self):
        try:
            logger.info("Fetching one record from the database.")
            return self.cursor.fetchone()
        except Exception as e:
            logger.info(f"Error fetching one record: {e}")
            raise
