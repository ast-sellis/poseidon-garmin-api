import time
from influxdb_client_3 import InfluxDBClient3, flight_client_options
import certifi

# InfluxDB connection setup
host = "https://us-east-1-1.aws.cloud2.influxdata.com"
token = "ak4zF9i0P3SuGasGtycWbF986bao9B-y58U6cgoeucbkpR_n8xTgZtoSPdbQcpZECPhb0V9KshCh-E_XR07xwA=="
org = "Technical Team"
database = "garmin"

with open(certifi.where(), "r") as fh:
    cert = fh.read()

fco = flight_client_options(
    tls_root_certs=cert,
    
    )

client = InfluxDBClient3(
    host=host,
    token=token,
    org=org,
    database="garmin",
    flight_client_options=fco,

)

query = """
SELECT 
    "latitudeInDegree", "longitudeInDegree", "time", "activityId"
FROM "garmin_activity_samples"
WHERE
    time >= TIMESTAMP '2024-11-28T00:00:00Z'
    AND time <= TIMESTAMP '2024-11-28T23:59:59Z'
    AND "userId" IN ('7b0b96a0-6ef5-4304-8971-81d74a50be33')
"""

start_time = time.time()
result = client.query(query)
end_time = time.time()

print(f"Query execution time: {end_time - start_time:.3f}s")
print(f"Records fetched: {len(result.to_pylist())}")