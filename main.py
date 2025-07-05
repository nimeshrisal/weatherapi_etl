from config.config import Config
from etl.extract import WeatherExtractor
from etl.transform_stage import WeatherTransformer
from etl.transform_final import WeatherTransformerFinal

def main():
    config = Config()

    # Step 1: Extract
    extractor = WeatherExtractor(config)
    extractor.fetch_and_store_raw()

    # Step 2: Transform
    transformer = WeatherTransformer(config)
    transformer.transform_raw_to_stage()

    # Step 3: Final Transform
    final_transformer = WeatherTransformerFinal(config)
    final_transformer.transform_stage_to_final()

if __name__ == "__main__":
    main()
