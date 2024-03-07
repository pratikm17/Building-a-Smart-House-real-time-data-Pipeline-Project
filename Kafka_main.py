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
ENVIRONMENTAL_TOPIC = os.getenv('ENVIRONMENTAL_TOPIC', 'environmental_data')
VOICE_COMMANDS_TOPIC = os.getenv('VOICE_COMMANDS_TOPIC', 'voice_commands_data')
CAMERA_TOPIC = os.getenv('CAMERA_TOPIC', 'camera_data')


def get_next_time(start_time):
    start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
    return start_time

def generate_Environmental_Monitoring_data(device_id, timestamp):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),  # percentage
        'airQualityIndex': random.uniform(0, 500)  # AQL Value goes here
    }

def generate_camera_data(device_id, timestamp, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def generate_voice_commands(device_id, timestamp, num_commands):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp,
        'command': random.choice(['Turn on lights', 'Increase volume', 'Pause music', 'open the door', 'Close window', 'Turn on AC', 'Turn off TV']),
    } 

def simulate_vehicle_movement(start_location):
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    
    # Ensure latitude and longitude are within valid ranges
    start_location['latitude'] = max(min(start_location['latitude'], BHOPAL_COORDINATES['latitude']), INDORE_COORDINATES['latitude'])
    start_location['longitude'] = max(min(start_location['longitude'], BHOPAL_COORDINATES['longitude']), INDORE_COORDINATES['longitude'])

    return start_location

def generate_vehicle_data(device_id, start_time, start_location):
    start_time = get_next_time(start_time)
    location = simulate_vehicle_movement(start_location)
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': start_time.isoformat(),
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
    start_time = datetime.now()
    start_location = INDORE_COORDINATES.copy()
    while True:
        vehicle_data = generate_vehicle_data(device_id, start_time, start_location)
        environmental_data = generate_Environmental_Monitoring_data(device_id, vehicle_data['timestamp'])
        voice_commands_data = generate_voice_commands(device_id, vehicle_data['timestamp'], 5)  
        camera_data = generate_camera_data(device_id, vehicle_data['timestamp'], 1)  

        if vehicle_data['location'][0] >= BHOPAL_COORDINATES['latitude'] \
                and vehicle_data['location'][1] >= BHOPAL_COORDINATES['longitude']:
            print('Vehicle has reached Bhopal. Simulation ending...')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, ENVIRONMENTAL_TOPIC, environmental_data)
        produce_data_to_kafka(producer, VOICE_COMMANDS_TOPIC, voice_commands_data)
        produce_data_to_kafka(producer, CAMERA_TOPIC, camera_data)

        time.sleep(25)

if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'python-producer'
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Pratik_Mahajan')

    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
