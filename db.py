import os
import psycopg2
import pandas as pd

# Database connection function
def get_db_connection():
    try:
        conn = psycopg2.connect(
    dbname="twitter_stream",
    user="postgres",
    password="disha@124",
    host="localhost",
    port="5432"
)

        return conn
    except Exception as e:
        print(f"❌ DB connection error: {e}")
        return None

# Function to insert a batch of hashtags
def insert_hashtag_data(df):
    conn = get_db_connection()
    if conn is not None:
        try:
            cur = conn.cursor()
            for _, row in df.iterrows():
                hashtag = row['word']
                count = int(row['count'])
                cur.execute("""
    INSERT INTO hashtag_counts (word, count) 
    VALUES (%s, %s)
    ON CONFLICT (word) 
    DO UPDATE SET count = EXCLUDED.count
""", (hashtag, count))


            conn.commit()
            cur.close()
            print(f"✅ Inserted {len(df)} rows.")
        except Exception as e:
            print(f"❌ Insert error: {e}")
        finally:
            conn.close()

# Traverse subfolders and process part-*.csv files
def process_csv_files(csv_root):
    for root, _, files in os.walk(csv_root):
        for file in files:
            if file.startswith("part-") and file.endswith(".csv"):
                filepath = os.path.join(root, file)
                print(f"📄 Reading file: {filepath}")
                try:
                    df = pd.read_csv(filepath, header=None, names=["word", "count"])
                    insert_hashtag_data(df)
                except Exception as e:
                    print(f"❌ Failed to read {filepath}: {e}")

if __name__ == "__main__":
    process_csv_files("output/hashtags")
