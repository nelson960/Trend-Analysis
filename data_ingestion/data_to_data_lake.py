import csv
import json
import os

def save_csv_to_data_lake(csv_file_path, json_file_path):
    """
    Save CSV data to the data lake in JSON format.

    Args:
        csv_file_path (str): Path to the CSV file.
        json_file_path (str): Path to save the JSON file.
    """
    try:
        # Read CSV file
        with open(csv_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]

        # Save to JSON
        with open(json_file_path, mode='w') as json_file:
            json.dump(data, json_file, indent=4)
        
        print(f"Data successfully saved to {json_file_path}.")

    except Exception as e:
        print(f"Error saving data to data lake: {e}")

if __name__ == "__main__":
    # Path to your CSV file
    csv_file_path = "/Users/nelson/py/ml_App/trend-analysis/temp/tweets_trend_analysis.csv"
    # Path to save the JSON file in the data lake
    json_file_path = "data_lake/raw_tweets/tweets_data.json"
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(json_file_path), exist_ok=True)
    
    save_csv_to_data_lake(csv_file_path, json_file_path)
