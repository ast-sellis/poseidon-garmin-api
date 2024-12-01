from flask import Blueprint, request
from flask_restful import Api, Resource, reqparse
import os
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv
import logging
from geojson import Feature, FeatureCollection, LineString
from datetime import datetime, timedelta
import time

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
query_api = influx_client.query_api()

# Create a blueprint for Garmin-related endpoints
garmin_geojson_bp = Blueprint("garmin_geojson", __name__, url_prefix="/garmin")
api = Api(garmin_geojson_bp)


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
                timestamp = record.get("_time")

                # Skip invalid records
                if not all([activity_id, latitude, longitude, activity_type]):
                    continue

                # Convert to floats for GeoJSON
                latitude = float(latitude)
                longitude = float(longitude)

                if activity_id not in activities:
                    activities[activity_id] = {
                        "coordinates": [],
                        "start_time": timestamp,
                        "end_time": timestamp,
                        "activity_type": activity_type,
                    }

                activities[activity_id]["coordinates"].append((longitude, latitude))
                # Update start and end times
                activities[activity_id]["start_time"] = min(activities[activity_id]["start_time"], timestamp)
                activities[activity_id]["end_time"] = max(activities[activity_id]["end_time"], timestamp)

            # Create GeoJSON LineString features
            features = []
            for activity_id, data in activities.items():
                # Filter out invalid points
                coordinates = [coord for coord in data["coordinates"] if None not in coord]
                if not coordinates:
                    continue

                # Create a LineString for the activity
                features.append(
                    Feature(
                        geometry=LineString(coordinates),
                        properties={
                            "activityId": activity_id,
                            "activityType": data["activity_type"],
                            "start_time": data["start_time"],
                            "end_time": data["end_time"],
                        },
                    )
                )

            # Create the FeatureCollection
            feature_collection = FeatureCollection(features)
            return feature_collection, 200, {"Content-Type": "application/json"}

        except Exception as e:
            logger.error(f"Error processing GeoJSON request for user {user_id} on {date_str}: {e}")
            return {"errorMessage": str(e), "status": "error"}, 500


api.add_resource(GarminActivityGeoJSON, "/activity/geojson")
