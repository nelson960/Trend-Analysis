import os
import pandas as pd

def load_data(file_path: str) -> pd.DataFrame:
    """
    Load raw data from a file in various formats.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    _, ext = os.path.splitext(file_path)
    file_type = ext[1:].lower()  # Remove the dot and convert to lowercase
    
    try:
        if file_type == 'parquet':
            df = pd.read_parquet(file_path, engine="pyarrow")
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'json':
            df = pd.read_json(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        return df

    except ImportError as e:
        raise ImportError(f"Missing package dependency: {e}")
    except PermissionError as e:
        raise PermissionError(f"Permission denied when accessing {file_path}: {e}")
    except Exception as e:
        raise ValueError(f"Data loading failed: {e}")
