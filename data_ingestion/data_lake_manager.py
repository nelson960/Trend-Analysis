import os 
import json
import csv
from datetime import datetime

DATA_LAKE_PATH = "data_lake"

def ensure_dir(path):
	os.makedirs(path, exist_ok=True)

def save_raw_data(data, filename):
	year = datetime.now().strftime("%Y")
	month = datetime.now().strftime("%m")
	dir_path = os.path.join(DATA_LAKE_PATH, "raw", year, month)
	ensure_dir(dir_path)

	file_path = os.path.join(dir_path, f"{filename}.json")
	with open(file_path, "w") as f:
		json.dump(data, f)
	print(f"Raw data saved to {file_path}")