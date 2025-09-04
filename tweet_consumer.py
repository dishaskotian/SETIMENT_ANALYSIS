from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'tweetsTopic3',
    bootstrap_servers='localhost:9092',
    group_id='tweet_group',
    auto_offset_reset='earliest'
)

for msg in consumer:
    print(f"Received: {msg.value.decode('utf-8')}")
