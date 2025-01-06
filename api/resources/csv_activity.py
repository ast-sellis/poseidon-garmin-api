from flask import Blueprint, request, jsonify
from flask_restful import Api, Resource
import pandas as pd
import os
import uuid
from influxdb_client_3 import InfluxDBClient3, flight_client_options
from dotenv import load_dotenv
import certifi
import logging
import re

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB setup
token = os.getenv("INFLUXDB_TOKEN")
org = "Technical Team"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

# Load SSL certificates
with open(certifi.where(), "r") as fh:
    cert = fh.read()

fco = flight_client_options(tls_root_certs=cert)

influx_client = InfluxDBClient3(
    host=host,
    token=token,
    org=org,
    flight_client_options=fco,
)

# Create a blueprint and API
csv_bp = Blueprint("csv_activity", __name__, url_prefix="/csv")
api = Api(csv_bp)

MAX_BATCH_SIZE = 500

# Define column mappings and unit conversions
COLUMN_MAPPING = {
    "timestamp": ["timestamp", "time", "date_time", "datetime"],
    "latitudeInDegree": ["latitude", "lat"],
    "longitudeInDegree": ["longitude", "lon", "long"],
    "speedMetersPerSecond": ["sog_kts", "speed_over_ground", "speed", "velocity", "speed_knots", "speed_mph", "speed_kmh", "speed_m_min"],
    "courseOverGround": ["cog", "course_over_ground"],
    "headingInDegreeTrue": ["hdg_true", "heading"],
    "roll": ["roll", "x_axis"],
    "pitch": ["pitch", "y_axis"]
}

CONVERSION_FACTORS = {
    "knots": 0.514444,
    "kts": 0.514444,
    "mph": 0.44704,
    "km/h": 0.277778,
    "kmh": 0.277778,
    "m/min": 0.0166667,
    "mps": 1.0
}

def to_camel_case(s):
    parts = re.split(r'[_\s]+', s)
    return parts[0] + ''.join(word.capitalize() for word in parts[1:])

def normalize_columns(df):
    normalized_columns = {}
    for col in df.columns:
        matched = False
        for normalized_name, aliases in COLUMN_MAPPING.items():
            if col.lower() in aliases:
                normalized_columns[col] = normalized_name
                matched = True
                break
        if not matched:
            normalized_columns[col] = to_camel_case(col)
    df.rename(columns=normalized_columns, inplace=True)
    return df

def dms_to_decimal(dms_str):
    try:
        dms_pattern = r"(\d+)[°d](\d+)'(\d+\.?\d*)\"?([NSEW])?"
        match = re.match(dms_pattern, dms_str.strip())
        if not match:
            raise ValueError(f"Invalid DMS format: {dms_str}")
        degrees, minutes, seconds, direction = match.groups()
        decimal = float(degrees) + float(minutes) / 60 + float(seconds) / 3600
        if direction and direction.upper() in ["S", "W"]:
            decimal *= -1
        return round(decimal, 6)
    except Exception as e:
        logger.warning(f"Could not convert DMS to decimal: {dms_str}. Error: {e}")
        return None

def normalize_lat_lon(row):
    for field in ["latitude", "longitude"]:
        if pd.notnull(row.get(field)):
            value = row[field]
            if isinstance(value, str) and re.search(r"[°d]", value):
                row[field] = dms_to_decimal(value)
            else:
                try:
                    row[field] = round(float(value), 6)
                except ValueError:
                    row[field] = None
    return row

def identify_speed_units(column_name):
    column_name = column_name.lower()
    if "knot" in column_name or "kts" in column_name:
        return "knots"
    if "mph" in column_name:
        return "mph"
    if "km/h" in column_name or "kmh" in column_name:
        return "km/h"
    if "m/min" in column_name or "meters_per_minute" in column_name:
        return "m/min"
    if "mps" in column_name or "meters_per_second" in column_name:
        return "mps"
    return None

def convert_speed_to_mps(value, unit):
    if pd.notnull(value) and unit in CONVERSION_FACTORS:
        return round(float(value) * CONVERSION_FACTORS[unit], 6)
    elif unit is None:
        logger.warning(f"Unknown speed unit for value: {value}. Skipping conversion.")
        return None
    return None

def write_in_batches(client, database, records, precision):
    for i in range(0, len(records), MAX_BATCH_SIZE):
        batch = records[i:i + MAX_BATCH_SIZE]
        client.write(database=database, record=batch, write_precision=precision)

class CSVActivity(Resource):
    def post(self):
        try:
            if 'file' not in request.files or 'userId' not in request.form:
                return {"errorMessage": "File and userId are required.", "status": "error"}, 400
            
            uploaded_file = request.files['file']
            user_id = request.form['userId']

            if not uploaded_file.filename.endswith(".csv"):
                return {"errorMessage": "Invalid file format. Please upload a CSV file.", "status": "error"}, 400
            
            activity_id = str(uuid.uuid4())
            df = pd.read_csv(uploaded_file)
            logger.info(f"CSV loaded successfully with {len(df)} rows.")
            
            df = normalize_columns(df)
            df = df.apply(normalize_lat_lon, axis=1)

            if "timestamp" not in df.columns:
                return {"errorMessage": "The CSV file must contain a timestamp column.", "status": "error"}, 400

            speed_column = None
            speed_unit = None
            for col in df.columns:
                if col in COLUMN_MAPPING["speedMetersPerSecond"]:
                    speed_column = col
                    speed_unit = identify_speed_units(col)
                    break

            if speed_column:
                logger.info(f"Identified speed column: {speed_column} with unit: {speed_unit}")
                df["speedMetersPerSecond"] = df[speed_column].apply(lambda x: convert_speed_to_mps(x, speed_unit))
                df.drop(columns=[speed_column], inplace=True)

            influx_points = []
            for _, row in df.iterrows():
                try:
                    time = str(row["timestamp"])
                    fields = {col: float(row[col]) for col in df.columns if col != "timestamp" and pd.notnull(row[col])}
                    point = {
                        "measurement": "csv_activity",
                        "tags": {
                            "userId": user_id,
                            "activityId": activity_id,
                            "device": uploaded_file.filename
                        },
                        "fields": fields,
                        "time": time
                    }
                    influx_points.append(point)
                except Exception as row_error:
                    logger.error(f"Error processing row: {row}. Error: {row_error}")

            if influx_points:
                write_in_batches(influx_client, "csv_uploads", influx_points, "ms")
                logger.info(f"Successfully wrote {len(influx_points)} points to InfluxDB.")

            return {"message": "CSV data processed and stored successfully", "activityId": activity_id, "status": "success"}, 200
        
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            return {"errorMessage": str(e), "status": "error"}, 500

# Add the resource to the API
api.add_resource(CSVActivity, "/upload")
