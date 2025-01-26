import os 
import logging
import pandas as pd
from datetime import datetime
from django.conf import settings
from typing import Optional, Union

logger = logging.getLogger(__name__)

def load_raw_data(file_path) ->pd.DataFrame:
	"""
    Load raw data from a Parquet file in the data lake.

    Args:
        file_path (str): Absolute path to the Parquet file.

    Returns:
        pd.DataFrame: Normalized data from the Parquet file.

    """
	try:
		#validate the file existence
		if not os.path.exists(file_path):
			raise FileNotFoundError(f"File not found: {file_path}")

        #Read data from parquet file with error checking
		df = pd.read_parquet(file_path, engine="pyarrow")
		if df.empty:
			logger.warning(f"Empty DataFrame loaded from {file_path}")
		return df
	except ImportError:
		logger.error("PyArrow not installed. Install with: pip install pyarrow")
		raise
	except PermissionError:
		logger.error(f"Permission denied when accessing {file_path}")
		raise
	except Exception as e :
		logger.error(f"Unexpected error loading data from {file_path}: {e}")
		raise ValueError(f"Data loading failed: {e}")


def save_to_data_lake(processed_data, filename, folder="processed")-> str:
	
    """
    Save processed tweet data to the data lake in Parquet format.

    Args:
        processed_data (pd.DataFrame): DataFrame containing processed tweet data.
        filename (str): The filename to save (without extension).
        folder (str): Subfolder inside data lake to save data (default: 'processed').

    Returns:
        str: Full path where the file was saved.
    """

    try:
        #Validate input 
        if processed_data is None or processed_data.empty:
            raise ValueError("Cannot save empty DataFrame")
        
        #Use django settings or default path
        data_lake_base_path = getattr(settings, "DATA_LAKE_PATH", "data_lake")
        
        #Create timestamp-based directory structure
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        dir_path = os.path.join(data_lake_base_path, folder, year, month)
        os.makedirs(dir_path, exist_ok=True)

        #Generate unique filename if needed
        unique_filename = f"{filename}_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
        file_path = os.path.join(dir_path, unique_filename)
        
        #Save processed data
        processed_data.to_parquet(
            file_path,
            engine ="pyarrow",
            index = False,
            compression='snappy') #Efficient compression 
        
        logger.info(f"Processed data saved: {file_path}")
        return file_path
	
    except PermissionError:
            logger.error(f"Permission denied when saving to {dir_path}")
            raise
    except Exception as e:
            logger.error(f"Data lake save failed: {e}")
            raise ValueError(f"Data lake save error: {e}")

    