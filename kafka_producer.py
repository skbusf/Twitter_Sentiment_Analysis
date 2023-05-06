from kafka import KafkaProducer
import time
import pandas as pd

kafka_bootstrap_server_cons = 'localhost:9092'
kafka_topic_name_cons = 'big_data_project'

if __name__ == "__main__":
    
    # Set up Kafka producer connection
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server_cons, value_serializer=lambda v: str(v).encode('utf-8'))

    # Read data from CSV file
    tweets_data = pd.read_csv("Twitter_Jan_Mar_cleaned.csv")

    # Convert data to dictionary
    tweets_list = tweets_data.to_dict(orient = 'records')

    # Read data from CSV file
    for tweet in tweets_list:
        producer.send(kafka_topic_name_cons, tweet)
        time.sleep(0.5)
