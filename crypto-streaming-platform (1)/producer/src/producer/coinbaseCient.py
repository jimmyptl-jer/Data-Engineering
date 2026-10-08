from coinbase.websocket import (
    WSClient,
    WSClientConnectionClosedException,
    WSClientException,
)

def coibaseClient():
    # TODO: Define or load these values
    api_key = "89f36114-b77b-43d3-9a34-0cab5f6e8509"
    api_secret = "hRnGPmL1JWuh7jVY4kEdAM+D8bQApp4X2HBCzeXqmKSebdjkFQ8xqRRscpHKlJFG54TvIR/3AI1KzcV3fUHWoA=="

    # TODO: Implement this callback
    def on_message(message):
        print(message)

    client = WSClient(
        api_key=api_key,
        api_secret=api_secret,
        on_message=on_message,
    )

    try:
        client.open()
        client.subscribe(
            product_ids=["BTC-USD", "ETH-USD"],
            channels=["ticker", "heartbeats"],
        )
        client.run_forever_with_exception_check()

    except WSClientConnectionClosedException:
        print("Connection closed! Retry attempts exhausted.")

    except WSClientException as e:
        print(f"Error encountered: {e}")


