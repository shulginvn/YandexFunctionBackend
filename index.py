import json
import ssl
import asyncio
import paho.mqtt.client as mqtt
import requests
import boto3
import os
import base64
from botocore.config import Config
import xml.etree.ElementTree as ET

MQTT_BROKER = "mqtt.cloud.yandex.net"
CA_CERT_PATH = "rootCA.crt"
MQTT_PORT = 8883

MQTT_USERNAME = "arek49eikmn22u0es6k3"
MQTT_PASSWORD = "s/p4N;>?f-8j.2PtMLZR:e"

email_device_association = {}

file_key = 'email_device_association.json'
bucket_name = 'iotbacket'

boto_config = Config(
    signature_version = 's3'
)

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION'),
    config=boto_config
)

def load_associations():
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3.exceptions.NoSuchKey:
        return {}

def save_associations(data):
    try:      
        # Sending data to Object Storage
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps(data).encode('utf-8')
        )
        print("Ассоциации успешно сохранены в Object Storage.")
    except Exception as e:
        print(f"Ошибка при сохранении ассоциаций: {e}")

def generate_device_name(device_id, index):

    base_name = "Умная штора"
    
    name = f"{base_name} {index + 1}"
    
    if len(name) > 25:
        name = name[:25] 
    
    return name

def generate_topics(device_id):
    return {
        "commands_topic": f"$devices/{device_id}/commands",
        "events_topic": f"$devices/{device_id}/events"
    }

async def send_command_and_wait_response(device_id, value, timeout=3.0):

    response_received = None

    def on_message(client, userdata, message):
        nonlocal response_received
        print(f"Получено сообщение из топика {message.topic}: {message.payload.decode()}")
        response_received = message.payload.decode()

    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.tls_set(ca_certs=CA_CERT_PATH, cert_reqs=ssl.CERT_REQUIRED)
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)    
    client.loop_start()

    topics = generate_topics(device_id)

    client.subscribe(topics["events_topic"], qos=1)
    print(f"Подписались на топик: {topics['events_topic']}")

    # Publishing data to an event topic
    command_payload = json.dumps(value)  # Convert data to JSON
    client.publish(topics["commands_topic"], command_payload, qos=1)

    start_time = asyncio.get_event_loop().time()
    while (asyncio.get_event_loop().time() - start_time) < timeout:
        await asyncio.sleep(0.1)
        if response_received is not None:
            break
    
    print(f"Данные отправлены в топик {topics['commands_topic']}: {command_payload}")
    client.loop_stop()
    client.disconnect()

    return response_received

def handler(event, context):

    #s3.put_object(Bucket=bucket_name, Key="test.txt", Body="Hello, world!")

    ### Process get firmware
    if 'firmware' in event.get('queryStringParameters', {}):

        # Get the firmware file name from the request
        firmware_file = event['queryStringParameters']['firmware']

        try:
            # Get an object (firmware) from a bucket
            response = s3.get_object(Bucket=bucket_name, Key=firmware_file)
            firmware_data = response['Body'].read()

            firmware_base64 = base64.b64encode(firmware_data).decode('utf-8')

            # Return the firmware in response
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/octet-stream',
                    'Content-Disposition': f'attachment; filename="{firmware_file}"'
                },
                'body': firmware_base64,
                'isBase64Encoded': True
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': f'Error: {str(e)}'
            }        
    ###

    email_device_association = load_associations()
    
    # Printing the input query for debugging
    print("Входной запрос (event):", json.dumps(event, ensure_ascii=False))

    # Getting data from the input request
    http_method = event.get("httpMethod", "")
    if http_method == "POST":

        try:
            body = json.loads(event.get("body", "{}"))

            # Handling the case if body is a dictionary
            if isinstance(body, dict) and "firmwareVersion" in body:
                try:
                    # Getting the SystemManifest.xml file from S3
                    response = s3.get_object(Bucket=bucket_name, Key='SystemManifest.xml')
                    manifest_data = response['Body'].read().decode('utf-8')

                    # Parse XML
                    root = ET.fromstring(manifest_data)
                    version = root.find('.//Version').text

                    return {
                        'statusCode': 200,
                        'headers': {
                            'Content-Type': 'application/json'
                        },
                        'body': json.dumps({'version': version})
                    }
                except Exception as e:
                    return {
                        'statusCode': 500,
                        'body': json.dumps({'error': str(e)})
                    }

            # Handling the case if body is a list
            if isinstance(body, list):
                email = None
                device_id = None

                # First, we look for email and device_id separately
                for entry in body:
                    if not isinstance(entry, dict):
                        continue  # Skipping invalid elements

                    if "email" in entry:
                        email = entry["email"]
                    if "device_id" in entry:
                        device_id = entry["device_id"]

                # After going through the entire list, we link the email and device_id
                if email and device_id:
                    if email not in email_device_association:
                        email_device_association[email] = []

                    if device_id not in email_device_association[email]:
                        email_device_association[email].append(device_id)
                        print(f"Сохранена ассоциация: email={email}, device_id={device_id}")
                    else:
                        print(f"Устройство {device_id} уже связано с email {email}")

                    save_associations(email_device_association)

            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Processed successfully"})
            }

        except Exception as e:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": str(e)})
            }


    # Extracting data from a request
    request_id = event.get("headers", {}).get("request_id", "")
    request_type = event.get("request_type", "")
    payload = event.get("payload", {})
    devices = payload.get("devices", [])
    access_token = event.get("headers", {}).get("authorization", "")
    access_token = access_token.replace("Bearer ", "")

    # Checking for request_id
    if not request_id:
        print("Ошибка: request_id отсутствует в запросе")

    # Initialize default response
    response = {
        "request_id": request_id,
        "payload": {
            "devices": []
        }
    }

    if request_type == "action":  # Request to change state
        # Process each device 
        for device in devices:
            device_id = device.get("id")
            capabilities = device.get("capabilities", [])

            response_state = ""
            # Process each capability
            for capability in capabilities:
                capability_type = capability.get("type")
                state = capability.get("state", {})

                if capability_type == "devices.capabilities.on_off":
                    # On/Off handling
                    value = state.get("value")
                    print(f"Device {device_id}: on_off set to {value}")
                    response_state = asyncio.run(send_command_and_wait_response(device_id, "open" if value else "close"))
                if capability_type == "devices.capabilities.range":
                    # On/Off range
                    value = state.get("value")
                    print(f"Device {device_id}: on_off set to {value}")
                    response_state = asyncio.run(send_command_and_wait_response(device_id, value))

        response["payload"]["devices"].append({
            "id": device_id,
            "action_result": {
                "status": "ERROR"  # Confirm the execution of the action
            }
        })

        if response_state:
            try:
                state_data = json.loads(response_state)
                if "status" in state_data:
                    response["payload"]["devices"][0]["action_result"]["status"] = state_data["status"]
            except json.JSONDecodeError:
                print("Ошибка декодирования ответа от устройства")


    elif request_type == "query":  # Request to get device status
        # Process each device
        for device in devices:
            device_id = device.get("id")

            response["payload"]["devices"].append({
                "id": device_id,
                "query_result": {
                    "status": "DONE"  # Confirm the execution of the action
                }
            })

    elif request_type == "discovery":  # Request for list of devices

        def get_user_info(access_token):
            url = "https://login.yandex.ru/info"
            headers = {"Authorization": f"Bearer {access_token}"}
            user_info = requests.get(url, headers=headers)
            
            if user_info.status_code == 200:
                return user_info.json().get("default_email", "")
            return ""
        
        user_email = get_user_info(access_token)

        for email, devices in email_device_association.items():
            print(f"Email: {email}, Устройства: {devices}")
            
        print(user_email)

        if (user_email == ""):
            return {}
            
        user_device_ids = email_device_association.get(user_email, [])
        devices_list = []

        for index, device_id in enumerate(user_device_ids):
            devices_list.append({
                "id": device_id,
                "name": generate_device_name(device_id, index),
                "type": "devices.types.openable.curtain",
                "capabilities": [
                    {
                        "type": "devices.capabilities.on_off",
                        "retrievable": True
                    },
                    {
                        "type": "devices.capabilities.range",
                        "retrievable": True,
                        "parameters": {
                            "instance": "open",
                            "range": {
                                "min": 0,
                                "max": 100,
                                "precision": 1
                            },
                            "unit": "unit.percent"
                        }
                    }
                ]
            })

        response["payload"]["user_id"] = "user123"
        response["payload"]["devices"] = devices_list
    else:
        # If the request is not recognized, we return an error
        response = {
            "request_id": request_id,
            "payload": {
                "error_code": "INVALID_REQUEST",
                "error_message": "Unsupported request type"
            }
        }

    # Printing the response for debugging
    print("Ответ (response):", json.dumps(response, ensure_ascii=False))

    return response