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

mode=$1
endpoint=$2
username=$3
password=$4
schema_count=$5
: "${schema_count:=20}"
vus=$6
: "${vus:=10}"
duration=$7
: "${duration:="5m"}"
minio_url=$8
: "${minio_url:="localhost:9000"}"
minio_access_key=$9
: "${minio_access_key:="minioadmin"}"
minio_secret_key=${10}
: "${minio_secret_key:="minioadmin"}"
minio_bucket=${11}
: "${minio_bucket:="parseable"}"
ingestor_endpoint=${12}
ingestor_username=${13}
ingestor_password=${14}
edition=${QUEST_EDITION:-oss}
stream_name=$(head /dev/urandom | tr -dc a-z | head -c10)

case "$edition" in
  oss|enterprise) ;;
  *)
    echo "invalid QUEST_EDITION: $edition" >&2
    exit 1
    ;;
esac

configure_pb () {
  export XDG_CONFIG_HOME="${XDG_CONFIG_HOME:-/tmp/quest-pb-config}"
  pb profile add quest "$endpoint" "$username" "$password" -o json \
    && pb profile default quest -o json
}

run () {
  echo "Running $edition integration tests"
  if [ "$mode" != "load" ]; then
    run_tests "$mode"
    return
  fi

  batch_one='^(TestLoadStreamBatchWithCustomPartitionWithK6|TestLoadStreamNoBatchWithK6|TestLoadStreamNoBatchWithCustomPartitionWithK6)$'
  batch_two='^(TestLoadStreamBatchWithK6|TestLoadStreamBatchWithK6_StaticSchema)$'
  smoke_load='^TestSmokeLoadWithK6Streams$'
  load_tests='^(TestLoadStreamBatchWithCustomPartitionWithK6|TestLoadStreamNoBatchWithK6|TestLoadStreamNoBatchWithCustomPartitionWithK6|TestLoadStreamBatchWithK6|TestLoadStreamBatchWithK6_StaticSchema|TestSmokeLoadWithK6Streams)$'

  echo "Running functional and smoke tests in parallel"
  run_tests smoke -test.skip "$load_tests" || return $?

  echo "Running k6 load test batch 1"
  run_tests load -test.run "$batch_one" || return $?

  echo "Running k6 load test batch 2"
  run_tests load -test.run "$batch_two" || return $?

  echo "Running k6 smoke load test"
  run_tests smoke -test.run "$smoke_load"
}

run_tests () {
  test_mode=$1
  shift
  ./quest.test -test.v -test.parallel=64 "$@" -edition="$edition" -mode="$test_mode" -query-url="$endpoint" -stream="$stream_name" -query-user="$username" -query-pass="$password" -minio-url="$minio_url" -minio-user="$minio_access_key" -minio-pass="$minio_secret_key" -minio-bucket="$minio_bucket" -ingestor-url="$ingestor_endpoint" -ingestor-user="$ingestor_username" -ingestor-pass="$ingestor_password"
}

configure_pb || exit $?
run
