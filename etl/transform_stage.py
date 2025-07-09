import json 
from config.logs import logger
from db.connection import DatabaseConnection as DBConnection
from config.config import Config
from datetime import datetime

class WeatherTransformer:

    def __init__(self, config: Config):
        self.config = config
        
    def transform_raw_to_stage(self):
        try:
            with DBConnection(self.config) as db:
                logger.info("Fetching raw weather data from the database.")

                db.execute("SELECT max(id) FROM weather_raw")
                # latest_id = db.fetchone()['max']

                db.execute("SELECT id, city, raw_json FROM weather_raw")
                raw_records = db.fetchall()

                inserted = 0

                for records in raw_records:
                    try:
                        city = records['city']
                        raw = records['raw_json']

                        if isinstance(raw, str):
                            raw = json.loads(raw)
                        
                        #Extract fields

                        temperature = raw['temperature']
                        humidity = raw['humidity']
                        weather_description = raw['weather']
                        timestamp = raw['timestamp']
                        if timestamp:
                            if isinstance(timestamp, str):
                                timestamp = datetime.fromisoformat(timestamp)
                        # Insert into stage table
                        insert_query = """
                            INSERT INTO weather_staging (city, temperature, humidity, weather_description, timestamp)
                            VALUES (%s, %s, %s, %s, %s)
                            """
                        
                        db.execute(insert_query, (city, temperature, humidity, weather_description, timestamp))
                        inserted += 1
                        logger.info(f"Inserted data for city: {city}")  
                    except Exception as e:  
                        logger.error(f"Error processing record for city {city}: {e}")
                        
                logger.info(f"Total records inserted into stage table: {inserted}")

        except Exception as e:
            logger.error(f"Error transforming raw data to stage: {e}")
            raise e

