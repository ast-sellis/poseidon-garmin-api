from flask import Blueprint, request, jsonify
from flask_restful import Api, Resource
import os
from influxdb_client_3 import InfluxDBClient3, flight_client_options
from dotenv import load_dotenv
import certifi
import logging


load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB setup
token = os.getenv("INFLUXDB_TOKEN")
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
    database="garmin",
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
                influx_client.write(database="garmin", record=summary_point, write_precision="s")

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

            summary_points = []
            sample_points = []

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
                for sample in samples:
                    sample_point = {
                        "measurement": "garmin_activity_samples",
                        "tags": filter_tags({
                            "userId": detail.get("userId"),
                            "activityType": activity_type,
                            "activityId": summary.get("activityId"),
                        }),
                        "fields": filter_fields(sample, ["userId", "activityType", "activityId"]),
                        "time": sample.get("startTimeInSeconds"),
                    }
                    sample_points.append(sample_point)

            # Write the summary points in a single batch
            if summary_points:
                logger.info(f"Writing {len(summary_points)} summary points")
                write_in_batches(influx_client, "garmin", summary_points, "s")

            # Write the sample points in a single batch
            if sample_points:
                logger.info(f"Writing {len(sample_points)} sample points")
                write_in_batches(influx_client, "garmin", sample_points, "s")

            return {"message": "Activity details processed successfully", "status": "success"}, 200
        except Exception as e:
            logger.error(f"Error processing Garmin activity details: {e}")
            if "503" in str(e):
                return {"errorMessage": "Service temporarily unavailable. Please try again later.", "status": "error"}, 503
            return {"errorMessage": str(e), "status": "error"}, 500


# Add resources to the API
api.add_resource(GarminActivity, "/activity")
api.add_resource(GarminActivityDetails, "/activity/details")
