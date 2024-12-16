import os
import json
import requests
import time

# Read configuration from environment variables, with defaults
P_URL = os.getenv("P_URL", "https://demo.parseable.com:8000")
API_URL = P_URL + "/api/v1/query"
P_USERNAME = os.getenv("P_USERNAME", "admin")
P_PASSWORD = os.getenv("P_PASSWORD", "admin")
QUERY_FILE = os.getenv("QUERY_FILE", "queries.sql")
RESULT_FILE = os.getenv("RESULT_FILE", "result.csv")
ERROR_LOG = os.getenv("ERROR_LOG", "errors.log")

# Prepare the result CSV file
with open(RESULT_FILE, "w") as result_file:
    result_file.write("QueryNumber,ElapsedTime(ms)\n")

# Process queries
query_num = 1

with open(QUERY_FILE, "r") as query_file:
    for line in query_file:
        query = line.strip()
        if not query:  # Skip empty lines
            continue

        # Prepare the JSON payload
        payload = {
            "query": query,
            "startTime": "2024-11-29T00:00:00.000Z",
            "endTime": "2024-11-29T23:00:00.000Z"
        }

        # Log start time
        start_time = int(time.time() * 1000)

        print(f"{query} => ", end="", flush=True)
        try:
            response = requests.post(
                API_URL,
                json=payload,
                auth=(P_USERNAME, P_PASSWORD),
                headers={"Content-Type": "application/json"},
                verify=False
            )

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")

            # Log end time and calculate elapsed time
            end_time = int(time.time() * 1000)
            elapsed_time = end_time - start_time

            print(f"{response.text} ({elapsed_time}ms)")

            # Write results to CSV
            with open(RESULT_FILE, "a") as result_file:
                result_file.write(f"{query_num},{elapsed_time}\n")
        except Exception as e:
            print("Failed")
            with open(ERROR_LOG, "a") as error_log:
                error_log.write(f"Error querying {query_num}: {str(e)}\n")

        query_num += 1
