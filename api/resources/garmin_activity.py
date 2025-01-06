from flask import Blueprint, request, jsonify
from flask_restful import Api, Resource
import os
from influxdb_client_3 import InfluxDBClient3, flight_client_options
from dotenv import load_dotenv
import certifi
import logging
import json

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB setup
token = os.getenv("INFLUXDB_TOKEN")
logger.info(f"Token: {token}")
org = "Technical Team"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

with open(certifi.where(), "r") as fh:
    cert = fh.read()

fco = flight_client_options(
    tls_root_certs=cert,
)

influx_client = InfluxDBClient3(
    host=host,
    token=token,
    org=org,
    flight_client_options=fco,
)

# Create a blueprint for Garmin-related endpoints
garmin_bp = Blueprint("garmin", __name__, url_prefix="/garmin")
api = Api(garmin_bp)

ALLOWED_ACTIVITY_TYPES = set()

def is_activity_allowed(activity_type):
    return not ALLOWED_ACTIVITY_TYPES or activity_type in ALLOWED_ACTIVITY_TYPES

def filter_fields(data, exclude_keys):
    return {k: v for k, v in data.items() if k not in exclude_keys and v is not None}

def filter_tags(tags):
    return {k: v for k, v in tags.items() if v is not None}

MAX_BATCH_SIZE = 500

def write_in_batches(client, database, records, precision):
    for i in range(0, len(records), MAX_BATCH_SIZE):
        batch = records[i:i + MAX_BATCH_SIZE]
        client.write(database=database, record=batch, write_precision=precision)

class GarminActivity(Resource):
    def post(self):
        try:
            activities = request.json.get("activities", [])
            if not activities:
                return {"errorMessage": "No activities provided", "status": "error"}, 400

            for activity in activities:
                activity_type = activity.get("activityType")
                if not is_activity_allowed(activity_type):
                    continue

                summary_point = {
                    "measurement": "garmin_activity",
                    "tags": {
                        "userId": activity.get("userId"),
                        "activityType": activity_type,
                        "activityId": activity.get("activityId"),
                        "deviceName": activity.get("deviceName"),
                    },
                    "fields": filter_fields(activity, ["userId", "activityType", "activityId", "deviceName"]),
                    "time": activity.get("startTimeInSeconds"),
                }
                logger.info(f"Writing summary point: {summary_point}")
                influx_client.write(database="garmin_activities", record=summary_point, write_precision="s")

            return {"message": "Activity summaries processed successfully", "status": "success"}, 200
        except Exception as e:
            logger.error(f"Error processing Garmin activities: {e}")
            return {"errorMessage": str(e), "status": "error"}, 500


class GarminActivityDetails(Resource):
    def post(self):
        try:
            details = request.json.get("activityDetails", [])
            if not details:
                return {"errorMessage": "No activity details provided", "status": "error"}, 400

            # Write details to debug_output.json
            debug_output_path = "debug_output.json"
            with open(debug_output_path, "w") as debug_file:
                json.dump(details, debug_file, indent=4)
            
            

            summary_points = []
            sample_points = []

            def calculate_cog(lat1, lon1, lat2, lon2):
                """Calculate Course Over Ground (COG) in degrees between two points."""
                import math

                delta_lon = math.radians(lon2 - lon1)
                lat1 = math.radians(lat1)
                lat2 = math.radians(lat2)

                x = math.sin(delta_lon) * math.cos(lat2)
                y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(delta_lon))
                initial_bearing = math.atan2(x, y)

                # Convert radians to degrees and normalize to 0-360
                return (math.degrees(initial_bearing) + 360) % 360

            for detail in details:
                summary = detail.get("summary", {})
                samples = detail.get("samples", [])

                activity_type = summary.get("activityType")
                if not is_activity_allowed(activity_type):
                    continue

                # Prepare the summary point
                summary_point = {
                    "measurement": "garmin_activity_details",
                    "tags": filter_tags({
                        "userId": detail.get("userId"),
                        "activityType": activity_type,
                        "activityId": summary.get("activityId"),
                        "deviceName": summary.get("deviceName"),
                    }),
                    "fields": filter_fields(summary, ["userId", "activityType", "activityId", "deviceName"]),
                    "time": summary.get("startTimeInSeconds"),
                }
                summary_points.append(summary_point)

                # Prepare the sample points
                previous_sample = None
                for sample in samples:
                    latitude = sample.get("latitudeInDegree")
                    longitude = sample.get("longitudeInDegree")

                    # Calculate COG if the previous sample exists
                    if previous_sample and latitude is not None and longitude is not None:
                        prev_lat = previous_sample.get("latitudeInDegree")
                        prev_lon = previous_sample.get("longitudeInDegree")
                        if prev_lat is not None and prev_lon is not None:
                            cog = calculate_cog(prev_lat, prev_lon, latitude, longitude)
                        else:
                            cog = None
                    else:
                        cog = None  # First sample or missing coordinates

                    sample_point = {
                        "measurement": "garmin_samples",
                        "tags": filter_tags({
                            "userId": detail.get("userId"),
                            "activityType": activity_type,
                            "activityId": summary.get("activityId"),
                        }),
                        "fields": {
                            **filter_fields(sample, ["userId", "activityType", "activityId"]),
                            "courseOverGround": cog,
                        },
                        "time": sample.get("startTimeInSeconds"),
                    }
                    sample_points.append(sample_point)
                    previous_sample = sample  # Update the previous sample

            # Write the summary points in a single batch
            if summary_points:
                logger.info(f"Writing {len(summary_points)} summary points")
                write_in_batches(influx_client, "garmin_activities", summary_points, "s")

            # Write the sample points in a single batch
            if sample_points:
                logger.info(f"Writing {len(sample_points)} sample points")
                write_in_batches(influx_client, "garmin_activities", sample_points, "s")

            return {"message": "Activity details processed successfully", "status": "success"}, 200
        except Exception as e:
            logger.error(f"Error processing Garmin activity details: {e}")
            if "503" in str(e):
                return {"errorMessage": "Service temporarily unavailable. Please try again later.", "status": "error"}, 503
            return {"errorMessage": str(e), "status": "error"}, 500

# Add resources to the API
api.add_resource(GarminActivity, "/activity")
api.add_resource(GarminActivityDetails, "/activity/details")
