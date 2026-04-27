import logging
import os
import json
import time

from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

from pipeline.transforms import ParseEvent, EnrichWithFeatures, SerialiseToJson
from producer.models import TransactionAuthorisationEvent

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)



def process_message(raw: bytes, enricher: EnrichWithFeatures, serialiser: SerialiseToJson):
    # Parse
    event, is_dead_letter = ParseEvent().process(raw)
    if is_dead_letter:
        return None, True

    # Enrich
    enriched = enricher.process(event)
    if not enriched:
        return None, True

    # Serialise
    result = serialiser.process(enriched)
    return result, False


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    input_topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions-raw")
    output_topic = "transactions-enriched"
    dead_letter_topic = "transactions-dead-letter"

    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers=bootstrap,
        group_id="beam-enrichment-pipeline",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,   # raw bytes
        consumer_timeout_ms=-1,            # block forever
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v,      # already bytes
        linger_ms=5,
        compression_type="gzip",
    )

    enricher = EnrichWithFeatures()
    enricher.setup()
    serialiser = SerialiseToJson()

    stats = {"processed": 0, "enriched": 0, "dead_letters": 0}

    logger.info(
        f"Pipeline running — "
        f"input={input_topic}  "
        f"output={output_topic}"
    )
    logger.info("Waiting for messages...")  

    try:
        for message in consumer:
            logger.info(f"Got message from partition {message.partition}")  
            result, is_dead_letter = process_message(
                message.value, enricher, serialiser
            )
            stats["processed"] += 1

            if is_dead_letter:
                producer.send(dead_letter_topic, value=message.value)
                stats["dead_letters"] += 1
            else:
                producer.send(
                    output_topic,
                    key=message.key,
                    value=result,
                )
                stats["enriched"] += 1

            if stats["processed"] % 10 == 0:
                logger.info(
                    f"processed={stats['processed']}  "
                    f"enriched={stats['enriched']}  "
                    f"dead_letters={stats['dead_letters']}"
                )

    except KeyboardInterrupt:
        logger.info("Shutting down pipeline")
    finally:
        enricher.teardown()
        consumer.close()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()