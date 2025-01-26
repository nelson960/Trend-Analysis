import os
import pandas as pd 
from datetime import datetime
from django.conf import settings
import logging

#Initialize logger 
logger = logging.getLogger(__name__)


def ensure_dir(path):
    """
    Ensure that a directory exists. If not, create it.

    Args:
        path (str): Path to the directory.
    """
    os.makedirs(path, exist_ok=True)

def save_raw_data(data, filename, folder="raw"):
    """
    Save raw data as a Parquet file in the data lake.

    Args:
        data (list or dict): The raw data to save.
        filename (str): The filename (without extension) to save the data as.
        folder (str): The folder inside the data lake where data will be stored.
    """
    if not isinstance(data, (list, dict)):
        raise ValueError("Data must be list or a dictionary.")
    if not filename:
        raise ValueError("Filename must be provided.")
    
    #Define data lake base path from django settings 
    data_lake_base_path = getattr(settings, "DATA_LAKE_PATH", "data_lake")

    #Build the folder path dynamically based on the current year and month
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    dir_path = os.path.join(data_lake_base_path, folder, year, month)
    ensure_dir(dir_path)
    
    #File path for the JSON file
    file_path = os.path.join(dir_path, f"{filename}.parquet")

    try:
        #If the data is a list of dictionaries, convert it to a pandas DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame([data]) #wrap the dict as a list to convert it

        #save Dataframe as parquet file
        df.to_parquet(file_path, engine="pyarrow", index=False)
        logger.info(f"Raw data saved to {file_path}")
    except Exception as e:
        logger.error(f"Failed to save raw data to {file_path}: {e}")
        raise

    return file_path
