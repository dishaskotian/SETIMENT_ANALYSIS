# producer.py
import pandas as pd
from kafka import KafkaProducer
import json
import time
import sys

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load the CSV
try:
    df = pd.read_csv('tweet_data.csv', encoding='ISO-8859-1')
except Exception as e:
    print(f"[Error] Failed to read CSV: {e}")
    sys.exit(1)

# Print columns to debug
print(f"[Info] Loaded columns: {df.columns.tolist()}")

# Detect appropriate column names
possible_usernames = ['user', 'username', 'name']
possible_texts = ['tweet', 'text', 'content', 'message']

username_col = next((col for col in possible_usernames if col in df.columns), None)
text_col = next((col for col in possible_texts if col in df.columns), None)

if not username_col or not text_col:
    print(f"[Error] Couldn't find suitable 'username' or 'text' column in the CSV.")
    sys.exit(1)

print(f"[Info] Using columns: username='{username_col}', text='{text_col}'")

# Send tweets to Kafka topic
for index, row in df.iterrows():
    # Basic clean-up and skip invalid rows
    if pd.isna(row[username_col]) or pd.isna(row[text_col]):
        continue

    tweet = {
        "username": str(row[username_col]).strip(),
        "text": str(row[text_col]).strip()
    }

    # Send to correct topic (matches your Spark code)
    future = producer.send('tweetsTopic3', key=str(row[username_col]).encode('utf-8'), value=tweet)
    record_metadata = future.get(timeout=10)
    print(f"[Producer] Sent to partition {record_metadata.partition}: {tweet}")


    
    if index % 10 == 0:
        print(f"[Producer] Sent: {tweet}")
    
    time.sleep(1)  # Simulate streaming pace
