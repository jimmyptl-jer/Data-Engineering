import os
import json
import time

from coinbase.websocket import (
    WSClient,
    WSClientConnectionClosedException,
    WSClientException,
)
from confluent_kafka import Producer

# --------------------------------------------------------------------
# Kafka Configuration
# --------------------------------------------------------------------
config = {
    "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "SEB2YTKCRA5NV3M4",
    "sasl.password": "cflteU7ubHJA0cCYQQwTvhkGu+A79z/CcGRXPP2IgdMZ8zSSplVGX+5ObTSotsKg",
    "session.timeout.ms": 45000,
    "client.id": "ccloud-python-client-9e2445fb-87e6-45b3-b528-32e3d48c2367",
}

TOPIC = "crypto_data"


# --------------------------------------------------------------------
# Kafka Delivery Callback
# --------------------------------------------------------------------
def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Sent -> Topic: {msg.topic()}, "
            f"Partition: {msg.partition()}, "
            f"Offset: {msg.offset()}"
        )


# --------------------------------------------------------------------
# Create Kafka Producer
# --------------------------------------------------------------------
def create_producer():
    return Producer(config)


# --------------------------------------------------------------------
# Coinbase WebSocket
# --------------------------------------------------------------------
def coinbase_client(producer):

    api_key = "89f36114-b77b-43d3-9a34-0cab5f6e8509"
    api_secret = "hRnGPmL1JWuh7jVY4kEdAM+D8bQApp4X2HBCzeXqmKSebdjkFQ8xqRRscpHKlJFG54TvIR/3AI1KzcV3fUHWoA=="

    if not api_key or not api_secret:
        raise Exception(
            "Please set COINBASE_API_KEY and COINBASE_API_SECRET"
        )

    def on_message(raw_message):

        try:
            message = json.loads(raw_message)
            print(message)
        except json.JSONDecodeError:
            print(raw_message)
            return

        key = None

        events = message.get("events", [])

        if events:
            tickers = events[0].get("tickers", [])

            if tickers:
                key = tickers[0].get("product_id")

        producer.produce(
            topic=TOPIC,
            key=key,
            value=json.dumps(message),
            callback=delivery_callback,
        )

        producer.poll(0)

    client = WSClient(
        api_key=api_key,
        api_secret=api_secret,
        on_message=on_message,
    )

    try:

        print("Opening Coinbase websocket...")

        client.open()

        client.subscribe(
            product_ids=["BTC-USD", "ETH-USD"],
            channels=["ticker", "heartbeats"],
        )

        print("Listening for messages...")

        client.run_forever_with_exception_check()

    except KeyboardInterrupt:

        print("Stopping...")

    except WSClientConnectionClosedException:

        print("Websocket closed.")

    except WSClientException as e:

        print(e)

    finally:

        producer.flush()

        try:
            client.close()
        except Exception:
            pass


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():

    producer = create_producer()

    coinbase_client(producer)


if __name__ == "__main__":
    main()