import logging
from config.config import Config
from db.connection import DatabaseConnection as DBConnection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class WeatherTransformerFinal:

    def __init__(self, config: Config):
        self.config = config

    def transform_stage_to_final(self):
        try:
            with DBConnection(self.config) as db:
                logger.info("Fetching staged weather data from the database.")

                #fetch all staged data
                db.execute("SELECT * FROM weather_staging")
                staged_records = db.fetchall()

                if not staged_records:
                    logger.info("No staged records found.")
                    return
                inserted = 0
                skipped_count = 0

                for record in staged_records:
                    try:

                        city = record['city'].strip().lower()
                        weather_description = record['weather_description'].strip().lower()
                        temperature = float(record['temperature'])
                        humidity = int(record['humidity'])
                        timestamp = record['timestamp']
                        month = timestamp.replace(day=1).date().strftime('%B')

                        #filtering outliers
                        if not (0<= humidity <= 100 and -100 <= temperature <= 100):
                            logger.warning(f"Skipping outlier data for city: {city} with temperature: {temperature} and humidity: {humidity}")
                            skipped_count +=1
                            continue
                      
                        check_query = """
                            SELECT 1 from weather_final
                            WHERE city = %s AND timestamp = %s
                            """
                        
                        db.execute(check_query, (city, timestamp))
                        if db.fetchone():
                            logger.info(f"Data for city: {city} at timestamp: {timestamp} already exists in final table. Skipping insert.")
                            skipped_count += 1
                            continue

                        # Insert into final table
                        insert_query = """
                            INSERT INTO weather_final (city, month,temperature, humidity, weather_description, timestamp)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """
                        
                        db.execute(insert_query, (city,month, temperature, humidity, weather_description, timestamp))
                        inserted += 1
                        logger.info(f"Inserted data for city: {city}")
                    
                    except Exception as e:
                        logger.error(f"Error processing record for city {city}: {e}")
                        skipped_count += 1

                logger.info(f"Total records inserted into final table: {inserted}")
                logger.info(f"Total records skipped due to outliers or duplicates: {skipped_count}")
                db.execute("TRUNCATE TABLE weather_staging")
        except Exception as e:
            logger.error(f"Error transforming raw data to final: {e}")
            raise e