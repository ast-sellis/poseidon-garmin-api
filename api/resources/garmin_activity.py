from flask import Blueprint, request
from flask_restful import Api, Resource, reqparse
import os
from influxdb_client import InfluxDBClient, Point, WritePrecision
from dotenv import load_dotenv
import logging
from geojson import Feature, FeatureCollection, LineString
from datetime import datetime, timedelta
import time
import json

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB setup
token = os.getenv("INFLUXDB_TOKEN")
org = "Technical Team"
url = "https://us-east-1-1.aws.cloud2.influxdata.com"

# Initialize InfluxDB client
influx_client = InfluxDBClient(
    url=url,
    token=token,
    org=org,
)

# Create APIs
write_api = influx_client.write_api()
query_api = influx_client.query_api()

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

def write_in_batches(client, bucket, records, precision):
    for i in range(0, len(records), MAX_BATCH_SIZE):
        batch = records[i:i + MAX_BATCH_SIZE]
        client.write(bucket=bucket, org=org, record=batch, write_precision=precision)

class GarminActivity(Resource):
    def post(self):
        try:
            activities = request.json.get("activities", [])
            if not activities:
                return {"errorMessage": "No activities provided", "status": "error"}, 400

            points = []

            for activity in activities:
                activity_type = activity.get("activityType")
                if not is_activity_allowed(activity_type):
                    continue

                point = Point("garmin_activity") \
                    .tag("userId", activity.get("userId")) \
                    .tag("activityType", activity_type) \
                    .tag("activityId", activity.get("activityId")) \
                    .tag("deviceName", activity.get("deviceName")) \
                    .time(activity.get("startTimeInSeconds"), WritePrecision.S) \
                    .field("data", json.dumps(filter_fields(activity, ["userId", "activityType", "activityId", "deviceName"])))

                points.append(point)

            if points:
                logger.info(f"Writing {len(points)} activity summary points")
                write_api.write(bucket="garmin", org=org, record=points, write_precision=WritePrecision.S)

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
                summary_point = Point("garmin_activity_details") \
                    .tag("userId", detail.get("userId")) \
                    .tag("activityType", activity_type) \
                    .tag("activityId", summary.get("activityId")) \
                    .tag("deviceName", summary.get("deviceName")) \
                    .time(summary.get("startTimeInSeconds"), WritePrecision.S) \
                    .field("data", json.dumps(filter_fields(summary, ["userId", "activityType", "activityId", "deviceName"])))

                summary_points.append(summary_point)

                # Prepare the sample points
                for sample in samples:
                    sample_point = Point("garmin_activity_samples") \
                        .tag("userId", detail.get("userId")) \
                        .tag("activityType", activity_type) \
                        .tag("activityId", summary.get("activityId")) \
                        .time(sample.get("startTimeInSeconds"), WritePrecision.S)

                    # Add fields dynamically
                    for k, v in filter_fields(sample, ["userId", "activityType", "activityId"]).items():
                        sample_point.field(k, v)

                    sample_points.append(sample_point)

            # Write the summary points
            if summary_points:
                logger.info(f"Writing {len(summary_points)} summary points")
                write_in_batches(write_api, "garmin", summary_points, WritePrecision.S)

            # Write the sample points
            if sample_points:
                logger.info(f"Writing {len(sample_points)} sample points")
                write_in_batches(write_api, "garmin", sample_points, WritePrecision.S)

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

            # Build Flux query
            query = f'''
from(bucket: "garmin")
  |> range(start: {start_of_day.isoformat()}Z, stop: {end_of_day.isoformat()}Z)
  |> filter(fn: (r) => r["_measurement"] == "garmin_activity_samples")
  |> filter(fn: (r) => r["userId"] == "{user_id}")
  |> filter(fn: (r) => r["_field"] == "latitudeInDegree" or r["_field"] == "longitudeInDegree" or r["_field"] == "activityId" or r["_field"] == "activityType")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''

            logger.info(f"Executing query:\n{query}")
            start_time = time.time()
            result = query_api.query(query)
            end_time = time.time()
            logger.info(f"Query execution time: {end_time - start_time:.3f}s")

            # Process the results into GeoJSON format
            records = []
            for table in result:
                for record in table.records:
                    records.append(record.values)

            logger.info(f"Query returned {len(records)} records")

            # Process the results into GeoJSON format
            activities = {}
            for record in records:
                activity_id = record.get("activityId")
                latitude = record.get("latitudeInDegree")
                longitude = record.get("longitudeInDegree")
                activity_type = record.get("activityType")

                # Skip invalid records
                if not all([activity_id, latitude, longitude, activity_type]):
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
                        properties={"activityId": activity_id, "activityType": activity_type},
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
