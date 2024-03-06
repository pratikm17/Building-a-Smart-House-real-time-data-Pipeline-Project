import os
import time
import uuid
import random
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer
import json

INDORE_COORDINATES = {'latitude': 22.7196, 'longitude': -75.8577}
BHOPAL_COORDINATES = {'latitude': 23.2599, 'longitude': -77.4126}

LATITUDE_INCREMENT = (BHOPAL_COORDINATES['latitude'] - INDORE_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BHOPAL_COORDINATES['longitude'] - INDORE_COORDINATES['longitude']) / 100

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_incident_data')

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),  # km/h
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),  # percentage
        'airQualityIndex': random.uniform(0, 500)  # AQL Value goes here
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'incidentId': str(uuid.uuid4()),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }

def simulate_vehicle_movement():
    global start_location

    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    
    # Ensure latitude and longitude are within valid ranges
    start_location['latitude'] = max(min(start_location['latitude'], BHOPAL_COORDINATES['latitude']), INDORE_COORDINATES['latitude'])
    start_location['longitude'] = max(min(start_location['longitude'], BHOPAL_COORDINATES['longitude']), INDORE_COORDINATES['longitude'])

    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}")

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],
                                                                   vehicle_data['location'])

        if vehicle_data['location'][0] >= BHOPAL_COORDINATES['latitude'] \
                and vehicle_data['location'][1] >= BHOPAL_COORDINATES['longitude']:
            print('Vehicle has reached Bhopal. Simulation ending...')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(25)

if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'python-producer'
    }
    producer = SerializingProducer(producer_config)

    start_time = datetime.now()
    start_location = INDORE_COORDINATES.copy()

    try:
        simulate_journey(producer, 'vehicle-Pratik_Mahajan')

    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
