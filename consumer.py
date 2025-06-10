from confluent_kafka import Consumer

if __name__ == "__main__":

    config = {
        # User-specific properties that you must set
        "bootstrap.servers": "localhost:43235",
        # Fixed properties
        "group.id": "kafka-python-getting-started",
        "auto.offset.reset": "earliest",
    }

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = "test"
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                print(
                    "Consumed event from topic {topic}: key = {key} value = {value}".format(
                        topic=msg.topic(),
                        key=msg.key().decode("utf-8") if msg.key() else None,
                        value=msg.value().decode("utf-8") if msg.value() else None,
                    )
                )
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
