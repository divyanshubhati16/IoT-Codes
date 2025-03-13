import paho.mqtt.client as mqtt
import pandas as pd
import time
import json
import ssl

# AWS IoT details
ENDPOINT = "a2udxi009yj264-ats.iot.eu-central-1.amazonaws.com"  
THING_NAME = "fitnessdata7"
TOPIC = "newpolicy"  
CERTIFICATE_PATH = r"C:\Users\hp\AWS_Cloud\certificate.pem.crt"
PRIVATE_KEY_PATH = r"C:\Users\hp\AWS_Cloud\private.pem.key"
ROOT_CA_PATH = r"C:\Users\hp\AWS_Cloud\AmazonRootCA1.pem"

# MQTT Client setup
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=THING_NAME)
client.tls_set(ca_certs=ROOT_CA_PATH, certfile=CERTIFICATE_PATH, keyfile=PRIVATE_KEY_PATH, tls_version=ssl.PROTOCOL_TLSv1_2)

# Connect to AWS IoT Core
client.connect(ENDPOINT, 8883, 60)

# Function to read data from Excel and send to AWS IoT
def publish_data():
    while True:
        df = pd.read_excel(r"C:\Users\hp\AWS_Cloud\fitness_data.xlsx")  # Read Excel file
       
        for _, row in df.iterrows():
            payload = {
                "temperature_celsius": float(row["Temperature (°C)"]),  # Temperature in Celsius
                "speed_kmh": float(row["Speed (km/h)"]),  # Speed in km/h
                "heart_rate_bpm": int(row["Heart Rate (bpm)"]),  # Heart rate in bpm
                "calories_burned_kcal": float(row["Calories Burned (kcal)"]),  # Calories burned
                "timestamp": str(row["Timestamp"])  # Timestamp
            }

            client.publish(TOPIC, json.dumps(payload))  
            print(f"Published: {payload}")
            time.sleep(5)  # Adjust interval as needed

try:
 publish_data()
except KeyboardInterrupt:
    print("Stopped sending data")
    client.disconnect()