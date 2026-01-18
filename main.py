import base64
import json
from datetime import datetime
import functions_framework
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# Initialize the BigQuery client
client = bigquery.Client()

# Project and Table IDs
TABLE_ID = "smart-agri-iot-ass-2.agriculture_data.sensor_logs"

@functions_framework.cloud_event
def process_sensor_data(cloud_event):
    try:
        # 1. Decode the Pub/Sub message
        pubsub_data = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
        data = json.loads(pubsub_data)
        
        # 2. Extract values and add a timestamp
        row_to_insert = {
            "moisture": float(data.get('moisture', 0)),
            "temperature": float(data.get('temperature', 0)),
            "humidity": float(data.get('humidity', 0)),
            "timestamp": datetime.utcnow().isoformat()
        }

        # 3. Smart Agriculture Logic
        if row_to_insert["moisture"] < 400:
            print(f"ALERT: Soil dry ({row_to_insert['moisture']}). Triggering irrigation.")

        # 4. PREVENTIVE MEASURE: Try-Except block for Data Insertion
        try:
            errors = client.insert_rows_json(TABLE_ID, [row_to_insert])
            if errors == []:
                print(f"Successfully logged data point to BigQuery at {row_to_insert['timestamp']}")
            else:
                print(f"BigQuery insertion errors: {errors}")
        
        except GoogleAPIError as e:
            # Catches specific Google Cloud API issues (e.g., connection timeouts)
            print(f"Preventive Measure Triggered: Cloud API Error - {e}")
        
    except Exception as e:
        # Catches data formatting issues (e.g., if a sensor sends a string instead of a number)
        print(f"Critical System Error: Unexpected challenge addressed - {e}")

    return "OK"
