from confluent_kafka import Consumer, Producer, KafkaError
import json

# Kafka configuration for consumer and producer
consumer_conf = {
    'bootstrap.servers': 'broker:29092',
    'group.id': 'order-enrichment-group',
    'auto.offset.reset': 'earliest'
}

producer_conf = {
    'bootstrap.servers': 'broker:29092'
}

# Initialize Kafka consumer and producer
consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

# Subscribe to the 'orders' topic
consumer.subscribe(['orders'])

def process_order(order):
    try:
        # Extract the payload from the message
        payload = order.get('payload', {})
        
        # Get quantity and price from the payload
        quantity = payload.get('quantity', 0)
        price = payload.get('price', 0)
        
        if quantity > 0 and price > 0:
            # Calculate total order value and add it to the payload
            payload['total_value'] = quantity * price
            # Update the payload in the original order
            order['payload'] = payload
            print(f"Valid order processed: quantity={quantity}, price={price}, total_value={payload['total_value']}")
            return order, True  # Valid order
        else:
            print(f"Invalid order: quantity={quantity}, price={price}")
            return order, False  # Invalid order
    except Exception as e:
        print(f"Error processing order: {e}")
        print(f"Received order data: {order}")
        return order, False

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Start consuming messages
try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages with a 1 second timeout
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, nothing to do
                continue
            else:
                print(msg.error())
                break

        # Print received message for debugging
        print(f"Received message: {msg.value().decode('utf-8')}")
        
        try:
            # Parse the message as JSON
            order = json.loads(msg.value().decode('utf-8'))
            print(f"Parsed order: {order}")
            
            # Process and enrich the order
            enriched_order, is_valid = process_order(order)
            
            # Convert the enriched order back to JSON
            enriched_order_json = json.dumps(enriched_order).encode('utf-8')
            
            if is_valid:
                # Produce the valid enriched order to the 'enriched_orders' topic
                producer.produce('enriched_orders', value=enriched_order_json, callback=delivery_report)
            else:
                # Produce the invalid order to the 'invalid_orders' topic
                producer.produce('invalid_orders', value=enriched_order_json, callback=delivery_report)
            
            producer.flush()
        
        except json.JSONDecodeError as e:
            print(f"Failed to parse message as JSON: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error processing message: {e}")
            continue

finally:
    consumer.close()