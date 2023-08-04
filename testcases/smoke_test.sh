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

log_events=50
k6_log_events=60000
expectedCount=$((k6_log_events + log_events))

input_file=$PWD/input.json

alert_body='{"alerts":[{"message":"server side error occurred","name":"Status Alert","rule":{"config":{"column":"status","operator":"notEqualTo","repeats":2,"value":500},"type":"column"},"targets":[{"endpoint":"https://webhook.site/6b184e08-82c4-46dc-b344-5b85414c2a71","headers":{},"repeat":{"interval":"30s","times":5},"skip_tls_check":false,"type":"webhook"},{"endpoint":"https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX","repeat":{"interval":"3m 20s","times":5},"type":"slack"}]}],"version":"v1"}'

schema_body='{"fields":[{"name":"app_meta","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"bytes","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"datetime","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"device_id","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"host","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"level","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"location","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"message","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"method","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"os","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"p_metadata","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"p_tags","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"p_timestamp","data_type":{"Timestamp":["Millisecond",null]},"nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"process_id","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"protocol","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"referer","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"request","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"request_body","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"response_time","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"runtime","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"session_id","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"source_time","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"status","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"status_code","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"timezone","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"user-identifier","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"user_agent","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"user_id","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"uuid","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"version","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}}'

retention_body='[{"description":"delete after 20 days","action":"delete","duration":"20d"}]'

test_user="alice"
role_editor='[{"privilege": "editor"}]'
role_writer='[{"privilege": "writer", "resource": {"stream": "'"$stream_name"'"}}]'
role_reader='[{"privilege": "reader", "resource": {"stream": "'"$stream_name"'"}}]'

set_std_opts() {
  local username=$1
  local password=$2
  curl_std_opts=( -sS --header 'Content-Type: application/json' -w '\n\n%{http_code}' -u "$username":"$password" )
}

set_std_opts "$username" "$password"

# Generate events using flog (https://github.com/mingrammer/flog) and store it in input.json file
create_input_file () {
  flog -f json -n "$log_events" -t log -o "$input_file"
  sleep 2
  sed -i '1s/^/[/;$!s/$/,/;$s/$/]/' "$input_file"
  return $?
}

# stream does not exists
stream_does_not_exists() {
  response=$(curl "${curl_std_opts[@]}" --request POST "$parseable_url"/api/v1/logstream/"$stream_name" --data-raw "[{}]")

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 404 ]; then
    printf "Server returned http code: %s and response: %s\n" "$http_code" "$content"
    printf "Test stream_does_not_exists: failed\n"
    exit 1
  fi

  printf "Test stream_does_not_exists: successful\n"
  return 0
}

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

# Post log data to the stream
# first $log_events events using the flog tool
# then $k6_log_events events using the k6 tool
post_event_data () {
  create_input_file
  if [ $? -ne 0 ]; then
    printf "Failed to create log data to be posted to %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test post_event_data: failed\n"
    exit 1
  fi

  content=$(cat "$input_file")
  response=$(curl "${curl_std_opts[@]}" --request POST "$parseable_url"/api/v1/logstream/"$stream_name" --data-raw "$content")
  if [ $? -ne 0 ]; then
    printf "Failed to post log data to %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test post_event_data: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to create log stream %s with http code: %s and response: %s\n" "$stream_name" "$http_code" "$content"
    printf "Test post_event_data: failed\n"
    exit 1
  fi

  # Post k6 events for ~5 mins
  k6 run -e P_URL="$parseable_url" -e P_USERNAME="$username" -e P_PASSWORD="$password" -e P_STREAM="$stream_name" "/tests/testcases/smoke.js"
  if [ $? -ne 0 ]; then
    printf "Failed to post k6 events on %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test post_event_data: failed\n"
    exit 1
  fi

  printf "Test post_event_data: successful\n"
  return 0
}

# List all log stream and [TODO] verify if the stream is created
list_log_streams () {
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream)
  if [ $? -ne 0 ]; then
    printf "Failed to list log streams with exit code: %s\n" "$?"
    printf "Test list_log_streams: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to list all log streams with http code: %s and response: %s" "$http_code" "$content"
    printf "Test list_log_streams: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  echo "$content" > "$PWD/log_streams.json"

  if [ "$(jq < $PWD/log_streams.json '[.[].name | select(. == "'"$stream_name"'")] | length')" -ne 1 ]; then
    printf "Failed to find new log stream %s in list stream result: %s\n" "$stream_name" "$content"
    printf "Test list_log_streams: failed\n"
    exit 1
  fi

  printf "Test list_log_streams: successful\n"
  return 0
}

# Get Stream's schema and [TODO] validate its schema
get_streams_schema () {
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream/"$stream_name"/schema)
  if [ $? -ne 0 ]; then
    printf "Failed to fetch stream schema with exit code: %s\n" "$?"
    printf "Test get_streams_schema: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to get schema for stream %s with http code: %s and response: %s" "$stream_name" "$http_code" "$content"
    printf "Test get_streams_schema: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "$schema_body" ]; then
    printf "Get schema response doesn't match with expected schema.\n"
    printf "Schema expected: %s\n" "$schema_body"
    printf "Schema returned: %s\n" "$content"
    printf "Test get_streams_schema: failed\n"
    exit 1
  fi

  printf "Test get_streams_schema: successful\n"
  return 0
}

# Query the log stream and verify if count of events is equal to the number of events posted
query_log_stream() {
  # Query last 10 minutes of data only
  end_time=$(date "+%Y-%m-%dT%H:%M:%S%:z")
  start_time=$(date --date="@$(($(date +%s)-600))" "+%Y-%m-%dT%H:%M:%S%:z")

  response=$(curl "${curl_std_opts[@]}" --request POST "$parseable_url"/api/v1/query --data-raw '{
    "query": "select count(*) from '$stream_name'",
    "startTime": "'$start_time'",
    "endTime": "'$end_time'"
  }')
  if [ $? -ne 0 ]; then
    printf "Failed to query log data from %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test query_log_stream: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to query stream %s with http code: %s and response: %s" "$stream_name" "$http_code" "$response"
    printf "Test query_log_stream: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  queryResult=$(echo "$content" | cut -d ':' -f2 | cut -d '}' -f1)
  if [ "$queryResult" != "$expectedCount" ]; then
    printf "Validation failed. Count of events returned from query (%s) does not match total expected events (%s).\n" "$queryResult" "$expectedCount"
    printf "Test query_log_stream: failed\n"
    exit 1
  fi
  printf "Test query_log_stream: successful\n"
  return 0
}

# Set Alert
set_alert () {
  response=$(curl "${curl_std_opts[@]}" --request PUT "$parseable_url"/api/v1/logstream/"$stream_name"/alert --data-raw "$alert_body")
  if [ $? -ne 0 ]; then
    printf "Failed to set alert for %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test set_alert: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to set alert for %s with http code: %s and response: %s\n" "$stream_name" "$http_code" "$content"
    printf "Test set_alert: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "set alert configuration for log stream $stream_name" ]; then
    printf "Failed to set alert on log stream %s with response: %s\n" "$stream_name" "$content"
    printf "Test set_alert: failed\n"
    exit 1
  fi

  printf "Test set_alert: successful\n"
  return 0
}

# Get Alert
get_alert () {
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream/"$stream_name"/alert)
  if [ $? -ne 0 ]; then
    printf "Failed to get alert for %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test get_alert: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to get alert for %s with http code: %s and response: %s" "$stream_name" "$http_code" "$content"
    printf "Test get_alert: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "$alert_body" ]; then
    printf "Get alert response doesn't match with Alert config returned.\n"
    printf "Alert set: %s\n" "$alert_body"
    printf "Alert returned: %s\n" "$content"
    printf "Test get_alert: failed\n"
    exit 1
  fi

  printf "Test get_alert: successful\n"
  return 0
}

# Set Retention
set_retention () {
  response=$(curl "${curl_std_opts[@]}" --request PUT "$parseable_url"/api/v1/logstream/"$stream_name"/retention --data-raw "$retention_body")
  if [ $? -ne 0 ]; then
    printf "Failed to set retention for %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test set_retention: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to set retention for %s with http code: %s and response: %s\n" "$stream_name" "$http_code" "$content"
    printf "Test set_retention: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "set retention configuration for log stream $stream_name" ]; then
    printf "Failed to set retention on log stream %s with response: %s\n" "$stream_name" "$content"
    printf "Test set_retention: failed\n"
    exit 1
  fi

  printf "Test set_retention: successful\n"
  return 0
}

# Get Retention
get_retention () {
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream/"$stream_name"/retention)
  if [ $? -ne 0 ]; then
    printf "Failed to get retention for %s with exit code: %s\n" "$stream_name" "$?"
    printf "Test get_retention: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to get retention for %s with http code: %s and response: %s" "$stream_name" "$http_code" "$content"
    printf "Test get_retention: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "$retention_body" ]; then
    printf "Get retention response doesn't match with retention config returned.\n"
    printf "retention set: %s\n" "$retention_body"
    printf "retention returned: %s\n" "$content"
    printf "Test get_retention: failed\n"
    exit 1
  fi

  printf "Test get_retention: successful\n"
  return 0
}

# create User
put_user () {
  response=$(curl "${curl_std_opts[@]}" --request PUT "$parseable_url"/api/v1/user/"$test_user")
  if [ $? -ne 0 ]; then
    printf "Failed to create user %s with exit code: %s\n" "$test_user" "$?"
    printf "Test create_user: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to create user %s with http code: %s and response: %s\n" "$test_user" "$http_code" "$content"
    printf "Test set_retention: failed\n"
    exit 1
  fi

  # set curl options to user incluse test user and the passphrase
  test_password=$(sed -n '1p' <<< "$response")

  printf "Test create_user: successful\n"
  return 0
}

put_role() {
  local role=$1
  response=$(curl "${curl_std_opts[@]}" --request PUT "$parseable_url"/api/v1/user/"$test_user"/role --data-raw "$role")
  if [ $? -ne 0 ]; then
    printf "Failed put role for user %s with exit code: %s\n" "$test_user" "$?"
    printf "Test create_user: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to put role for user %s with http code: %s and response: %s\n" "$test_user" "$http_code" "$response"
    printf "Test set_retention: failed\n"
    exit 1
  fi

  printf "Test put_role: successful\n"
  return 0
}

# check api access for this new user
check_api_access() {
  # can call non protected api
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/liveness)
  if [ $? -ne 0 ]; then
    printf "Failed to get liveness api for new user with exit code: %s\n" "$?"
    printf "Test check_api_access: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to get liveness api for new user with http code: %s and response: %s", "$http_code" "$response"
    printf "Test check_api_access: failed\n"
    exit 1
  fi

  # can call protected api with access
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream)
  if [ $? -ne 0 ]; then
    printf "Failed to get logstream api for new user with exit code: %s\n" "$?"
    printf "Test check_api_access: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to get logstream api for new user with http code: %s and response: %s", "$http_code" "$response"
    printf "Test check_api_access: failed\n"
    exit 1
  fi

  # cannot call protected api without access
  response=$(curl "${curl_std_opts[@]}" --request DELETE "$parseable_url"/api/v1/logstream/"$stream_name")
  if [ $? -ne 0 ]; then
    printf "Failed when calling delete stream api for new user with exit code: %s\n" "$?"
    printf "Test check_api_access: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 401 ]; then
    printf "Delete api did not return unauthorized (403) for user %s, http code: %s and response: %s", "$test_user", "$http_code" "$response"
    printf "Test check_api_access: failed\n"
    exit 1
  fi

  printf "Test check_api_access: successful\n"
  return 0
}

delete_user() {
  response=$(curl "${curl_std_opts[@]}" --request DELETE "$parseable_url"/api/v1/user/"$test_user")
  if [ $? -ne 0 ]; then
    printf "Failed delete user %s with exit code: %s\n" "$test_user" "$?"
    printf "Test delete_user: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    printf "Failed to delete user %s with http code: %s and response: %s\n" "$test_user" "$http_code" "$content"
    printf "Test set_retention: failed\n"
    exit 1
  fi

  printf "Test delete_user: successful\n"
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

cleanup () {
  rm -rf "$input_file"
  rm -rf "$PWD/logstream_test.json"
  rm -rf "$PWD/log_streams.json"
  return $?
}

printf "======= Starting smoke tests =======\n"
printf "** Log stream name: %s **\n" "$stream_name"
printf "** Event count: %s **\n" "$expectedCount"
printf "====================================\n"
stream_does_not_exists
create_stream
post_event_data
list_log_streams
get_streams_schema
# sleep for a minute to ensure all data is pushed to backend
sleep 65
query_log_stream
set_alert
get_alert
set_retention
get_retention

put_user
put_role "$role_editor"
set_std_opts $test_user "$test_password"
check_api_access

set_std_opts $username "$password"
put_role "$role_writer"
set_std_opts $test_user "$test_password"
check_api_access

set_std_opts $username "$password"
put_role "$role_reader"
set_std_opts $test_user "$test_password"
check_api_access

set_std_opts $username "$password"
delete_user
delete_stream
cleanup
printf "======= Smoke tests completed ======\n"
