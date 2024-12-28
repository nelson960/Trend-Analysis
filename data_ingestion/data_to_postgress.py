import csv
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DB_PASS = os.getenv('TWITTER_DB')

def insert_data_to_db(csv_file_path, db_name="twitter_db", user="postgress", password=DB_PASS, host="localhost", port="5432"):
    """
    Insert data from a CSV file into the PostgreSQL database.

    Args:
        csv_file_path (str): Path to the CSV file.
        db_name (str): Database name.
        user (str): Database user.
        password (str): Database password.
        host (str): Database host.
        port (str): Database port.
    """
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        # Open the CSV file
        with open(csv_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Insert data into the database
                cursor.execute("""
                    INSERT INTO tweets (
                        date, like_count, reply_count, retweet_count, 
                        view_count, followers_count, tweets
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['date'], float(row['likeCount']), float(row['replyCount']),
                    float(row['retweetCount']), float(row['viewCount']),
                    int(row['followersCount']), row['tweets']
                ))

        # Commit the transaction
        conn.commit()
        print("Data successfully inserted into the database.")

    except Exception as e:
        print(f"Error inserting data into the database: {e}")
    finally:
        # Close the connection
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    # Path to your CSV file
    csv_file_path = "/Users/nelson/py/ml_App/trend-analysis/temp/tweets_trend_analysis.csv"
    insert_data_to_db(csv_file_path)
