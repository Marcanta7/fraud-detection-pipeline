from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import os
from dotenv import load_dotenv

load_dotenv()


def create_topics() -> None:
    client = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        client_id="fraud-pipeline-admin",
    )
    topics = [
        NewTopic(
            name=os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions-raw"),
            num_partitions=6,       # partition by customer_id later
            replication_factor=1,
        ),
        NewTopic(
            name=os.getenv("KAFKA_TOPIC_ALERTS", "fraud-alerts"),
            num_partitions=3,
            replication_factor=1,
        ),
    ]
    try:
        client.create_topics(topics)
        print("Topics created: transactions-raw, fraud-alerts")
    except TopicAlreadyExistsError:
        print("Topics already exist — skipping")
    finally:
        client.close()


if __name__ == "__main__":
    create_topics()