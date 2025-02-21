import os
import logging
import pandas as pd
from datetime import datetime
from django.conf import settings
from typing import Optional

logger = logging.getLogger(__name__)

def load_raw_data(file_path: str, file_format: str = "parquet", **kwargs) -> pd.DataFrame:
    """
    Load raw data from a file in the data lake.

    Args:
        file_path (str): Absolute path to the file.
        file_format (str): Format of the file. Supported formats: 'parquet', 'csv', 'json', 'excel'. Defaults to 'parquet'.
        **kwargs: Additional arguments passed to the respective pandas read function.

    Returns:
        pd.DataFrame: Loaded data as a pandas DataFrame.
    """
    try:
        # Validate that the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_format = file_format.lower()
        if file_format == "parquet":
            df = pd.read_parquet(file_path, engine=kwargs.pop("engine", "pyarrow"), **kwargs)
        elif file_format == "csv":
            df = pd.read_csv(file_path, **kwargs)
        elif file_format == "json":
            df = pd.read_json(file_path, **kwargs)
        elif file_format in ["excel", "xlsx", "xls"]:
            df = pd.read_excel(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        if df.empty:
            logger.warning(f"Empty DataFrame loaded from {file_path}")
        return df

    except ImportError:
        logger.error("Required library for the specified file format is not installed.")
        raise
    except PermissionError:
        logger.error(f"Permission denied when accessing {file_path}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading data from {file_path}: {e}")
        raise ValueError(f"Data loading failed: {e}")


def save_to_data_lake(processed_data: pd.DataFrame, filename: str, folder: str = "processed", file_format: str = "parquet", **kwargs) -> str:
    """
    Save processed data to the data lake in the specified format.

    Args:
        processed_data (pd.DataFrame): DataFrame containing processed data.
        filename (str): The base filename to save (without extension).
        folder (str): Subfolder inside data lake to save data (default: 'processed').
        file_format (str): Format to save the file. Supported formats: 'parquet', 'csv', 'json', 'excel'. Defaults to 'parquet'.
        **kwargs: Additional arguments passed to the respective pandas to_* function.

    Returns:
        str: Full path where the file was saved.
    """
    try:
        if processed_data is None or processed_data.empty:
            raise ValueError("Cannot save empty DataFrame")

        # Use django settings or default path
        data_lake_base_path = getattr(settings, "DATA_LAKE_PATH", "data_lake")

        # Create a timestamp-based directory structure
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        dir_path = os.path.join(data_lake_base_path, folder, year, month)
        os.makedirs(dir_path, exist_ok=True)

        # Determine file extension based on file format
        file_format = file_format.lower()
        if file_format == "parquet":
            ext = "parquet"
        elif file_format == "csv":
            ext = "csv"
        elif file_format == "json":
            ext = "json"
        elif file_format in ["excel", "xlsx", "xls"]:
            ext = "xlsx"
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        timestamp = now.strftime('%Y%m%d_%H%M%S')
        unique_filename = f"{filename}_{timestamp}.{ext}"
        file_path = os.path.join(dir_path, unique_filename)

        # Save the data using the appropriate pandas function
        if file_format == "parquet":
            processed_data.to_parquet(
                file_path,
                engine=kwargs.pop("engine", "pyarrow"),
                index=False,
                compression=kwargs.pop("compression", "snappy"),
                **kwargs
            )
        elif file_format == "csv":
            processed_data.to_csv(file_path, index=False, **kwargs)
        elif file_format == "json":
            processed_data.to_json(file_path, orient=kwargs.pop("orient", "records"), lines=kwargs.pop("lines", True), **kwargs)
        elif file_format in ["excel", "xlsx", "xls"]:
            processed_data.to_excel(file_path, index=False, **kwargs)
        else:
            # This branch should not be reached because of the check above
            raise ValueError(f"Unsupported file format: {file_format}")

        logger.info(f"Processed data saved: {file_path}")
        return file_path

    except PermissionError:
        logger.error(f"Permission denied when saving to {dir_path}")
        raise
    except Exception as e:
        logger.error(f"Data lake save failed: {e}")
        raise ValueError(f"Data lake save error: {e}")
