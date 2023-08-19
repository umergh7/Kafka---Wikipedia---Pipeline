from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import argparse
from sseclient import SSEClient
import json

def create_kafka_producer(bootstrap_server: str, acks:str):
    try:
        producer = KafkaProducer(
            bootstrap_servers = bootstrap_server,
            acks = acks,
            value_serializer = lambda x: json.dumps(x).encode('utf-8'))
    
    except NoBrokersAvailable:
        print('No broker found at {}'.format(bootstrap_server))
        raise

    if producer.bootstrap_connected():
        print('Producer Connected')
        return producer
    else:
        print('Failed to establish connection')
        exit(1)


def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')

    parser.add_argument('--bootstrap_server', default='localhost:9092',
                        help='Kafka bootstrap broker(s) (host[:port])', type=str)
    parser.add_argument('--topic_name', default='wikipedia-events',
                        help='Destination topic name', type=str)
    parser.add_argument('--acks', default='all',
                        help='Kafka Producer acknowledgment', type=str)
    parser.add_argument('--events_to_produce',
                        help='Kill producer after n events have been produced', type=int, default=1000)

    return parser.parse_args()


args = parse_command_line_arguments()

producer = create_kafka_producer(args.bootstrap_server, args.acks)

url = 'https://stream.wikimedia.org/v2/stream/recentchange'

message_count = 0

for event in SSEClient(url):
    if event.event == 'message' and event.data != '':
        event_data = json.loads(event.data)
        producer.send(args.topic_name, value=event_data)
        message_count += 1

        if message_count >= args.events_to_produce:
            print(f'Producer will be killed as {args.events_to_produce} events were produced')
            exit(0)













