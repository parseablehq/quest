#!/bin/sh
#
# Parseable Server (C) 2023 Cloudnatively Pvt. Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

parseable_url=$1
stream_name=$2
username=$3
password=$4
count=$5
vus=$6
duration=$7

curl_std_opts=( -sS --header 'Content-Type: application/json' -w '\n\n%{http_code}' -u "$username":"$password" )

# Create stream
create_stream () {
  response=$(curl "${curl_std_opts[@]}" --request PUT "$parseable_url"/api/v1/logstream/"$stream_name")
  
  if [ $? -ne 0 ]; then
    printf "Failed to create log stream %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test create_stream: failed\n"
    exit 1
  fi
  
  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to create log stream %s with http code: %s and response: %s\n" "$stream_name" "$http_code" "$content"
    printf "Test create_stream: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "log stream created" ]; then
    printf "Failed to create log stream $stream_name with response: %s\n" "$content"
    printf "Test create_stream: failed\n"
    exit 1
  fi

  printf "Test create_stream: successful\n"
  return 0
}

# Delete stream
delete_stream () {
  response=$(curl "${curl_std_opts[@]}" --request DELETE "$parseable_url"/api/v1/logstream/"$stream_name")

  if [ $? -ne 0 ]; then
    printf "Failed to delete stream for %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test delete_stream: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to delete log stream %s with http code: %s and response: %s\n" "$stream_name" "$http_code" "$content"
    printf "Test delete_stream: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "log stream $stream_name deleted" ]; then
    printf "Failed to delete log stream %s with response: %s" "$stream_name" "$content"
    printf "Test delete_stream: failed\n"
    exit 1
  fi

  printf "Test delete_stream: successful\n"
  return 0
}

run_k6() {
  printf "K6 script with 100 batched log events per HTTP Call"
  k6 run -e P_URL="$parseable_url" -e P_STREAM="$stream_name" -e P_USERNAME="$username" -e P_PASSWORD="$password" -e P_SCHEMA_COUNT="$count" "/tests/testcases/load_batch_events.js" --vus="$vus" --duration="$duration"

  printf "K6 script with 1 event per HTTP Call"
  k6 run -e P_URL="$parseable_url" -e P_STREAM="$stream_name" -e P_USERNAME="$username" -e P_PASSWORD="$password" -e P_SCHEMA_COUNT="$count" "/tests/testcases/load_single_event.js" --vus="$vus" --duration="$duration"
}

printf "======= Starting load tests with k6 =======\n"
printf "** Log stream name: %s **\n" "$stream_name"
printf "====================================\n"
create_stream
run_k6
delete_stream
printf "======= Load tests completed ======\n"
