import csv
from typing import List, Dict
from confluent_kafka import Producer, KafkaError, KafkaException

from ride import RideFHVSimple, RideGreenSimple, Ride
from settings import BOOTSTRAP_SERVERS, CLUSTER_API_KEY, CLUSTER_API_SECRET, INPUT_DATA_PATH_FHV, INPUT_DATA_PATH_GREEN, KAFKA_TOPIC_FHV, KAFKA_TOPIC_GREEN 


class CSVProducer(Producer):
    def __init__(self, props: Dict):
        self.producer = Producer(**props)

    @staticmethod
    def read_records(resource_path: str, RideClass=Ride):
        records = []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header row
            for row in reader:
                records.append(RideClass(arr=row))
        return records

    def publish_rides(self, topic: str, messages: List[Ride]):
        for ride in messages:
            try:
                self.producer.produce(topic=topic, key=str(ride.pu_location_id), value=ride.whole_record )
                #print('Record {} successfully produced at offset {}'.format(ride.pu_location_id, record.get().offset))
                #self.producer.flush()
            except KafkaError as e:
                print(e.__str__())
        self.producer.flush()


if __name__ == '__main__':
    # Config Should match with the KafkaProducer expectation
    kafka_config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': CLUSTER_API_KEY,
        'sasl.password': CLUSTER_API_SECRET
    }

    producer = CSVProducer(props=kafka_config)
    rides_fhv = producer.read_records(INPUT_DATA_PATH_FHV, RideFHVSimple)
    producer.publish_rides(topic=KAFKA_TOPIC_FHV, messages=rides_fhv)
    rides_green = producer.read_records(INPUT_DATA_PATH_GREEN, RideGreenSimple)
    producer.publish_rides(topic=KAFKA_TOPIC_GREEN, messages=rides_green)


