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
stream_name=$(head /dev/urandom | tr -dc a-z | head -c10)

run () {
  ./quest.test -test.v -mode="$mode" -query-url="$endpoint" -stream="$stream_name" -query-user="$username" -query-pass="$password" -minio-url="$minio_url" -minio-user="$minio_access_key" -minio-pass="$minio_secret_key" -minio-bucket="$minio_bucket" -ingestor-url="$ingestor_endpoint" -ingestor-user="$ingestor_username" -ingestor-pass="$ingestor_password"
  return $?
}

run
