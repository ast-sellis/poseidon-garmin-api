from flask import Blueprint, request, jsonify
from flask_restful import Api, Resource
import os
from influxdb_client_3 import InfluxDBClient3, flight_client_options
from dotenv import load_dotenv
import certifi
import logging
from geojson import Feature, FeatureCollection, LineString
from datetime import datetime, timedelta
from flask_restful import reqparse
import time

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


class GarminActivityGeoJSON(Resource):
    def get(self):
        """
        Handle GET requests to retrieve GeoJSON FeatureCollection for a user's activities on a given day.
        """
        user_id = None
        date_str = None

        try:
            # Parse query parameters
            parser = reqparse.RequestParser()
            parser.add_argument("userId", type=str, required=True, help="userId is required", location="args")
            parser.add_argument("date", type=str, required=True, help="date (YYYY-MM-DD) is required", location="args")
            args = parser.parse_args()

            user_id = args["userId"]
            date_str = args["date"]

            logger.info(f"Retrieving GeoJSON for user {user_id} on {date_str}")

            # Convert date string to datetime object
            date = datetime.strptime(date_str, "%Y-%m-%d")
            start_of_day = datetime(date.year, date.month, date.day)
            end_of_day = start_of_day + timedelta(days=1)

            # Query the database
            query = f"""
                SELECT 
                    "latitudeInDegree", "longitudeInDegree", "time", "activityId"
                FROM "garmin_activity_samples"
                WHERE
                    time >= TIMESTAMP '{start_of_day.isoformat()}Z'
                    AND time <= TIMESTAMP '{end_of_day.isoformat()}Z'
                    AND "userId" IN ('{user_id}')
                """
            logger.info(f"Executing query:\n{query}")
            start_time = time.time()  # Start timer
            result = influx_client.query(query)
            end_time = time.time()  # End timer

            logger.info(f"Query execution time: {end_time - start_time:.3f}s")

            # Convert query result to a Python list
            records = result.to_pylist()
            logger.info(f"Query returned {len(records)} records")

            # Process the results into GeoJSON format
            activities = {}
            for record in records:
                # logging.info(f"Record: {record}")
                # Extract relevant fields dynamically
                activity_id = record.get("activityId")
                latitude = record.get("latitudeInDegree")
                longitude = record.get("longitudeInDegree")
                time_field = record.get("time")

                # Skip invalid records
                if not all([activity_id, latitude, longitude, time_field]):
                    continue

                # Convert to floats for GeoJSON
                latitude = float(latitude)
                longitude = float(longitude)

                if activity_id not in activities:
                    activities[activity_id] = []

                activities[activity_id].append((longitude, latitude))

            # Create GeoJSON LineString features
            features = []
            for activity_id, coordinates in activities.items():
                # Filter out invalid points
                coordinates = [coord for coord in coordinates if None not in coord]
                if not coordinates:
                    continue

                # Create a LineString for the activity
                features.append(
                    Feature(
                        geometry=LineString(coordinates),
                        properties={"activityID": activity_id},
                    )
                )

            # Create the FeatureCollection
            feature_collection = FeatureCollection(features)
            return feature_collection, 200, {"Content-Type": "application/json"}

        except Exception as e:
            logger.error(f"Error processing GeoJSON request for user {user_id} on {date_str}: {e}")
            return {"errorMessage": str(e), "status": "error"}, 500

# Add resources to the API
api.add_resource(GarminActivity, "/activity")
api.add_resource(GarminActivityDetails, "/activity/details")
api.add_resource(GarminActivityGeoJSON, "/activity/geojson")