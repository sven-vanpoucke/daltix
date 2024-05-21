from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import datetime
import logging

def load_data(engine, key, query):
    try:
        if not isinstance(query, str):
            raise TypeError("Query must be a string.")
        logging.info(f"Querying started for {key}.")
        start_time = datetime.now()

        df = pd.read_sql(query, engine)

        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Querying finished for {key}. Duration: {duration}.")
        
        if df.empty:
            logging.warning(f"The result for query {key} is empty.")
        else:
            logging.info(f"Successfully loaded data for {key}.")
        return df
    except SQLAlchemyError as e:
        logging.error(f"Error querying data for {key}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error
    except TypeError as e:
        logging.error(f"Error with query {key}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error