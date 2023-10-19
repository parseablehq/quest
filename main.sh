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

schema_count="${5:-20}"
vus="${6:-10}"
duration="${7:-5m}"

# todo: accept all these as named argument instead: eg: -pan_user admin
panorama_address="${8:-http:0.0.0.0:5000}"
panorama_admin_username="${9:-pano_admin}"
panorama_admin_password="${10:-pano_admin}"

stream_name=$(head /dev/urandom | tr -dc a-z | head -c10)

run () {
  ./quest.test -test.v -mode="$mode" -url="$endpoint" -stream="$stream_name" -user="$username" -pass="$password" \
    -panorama-address="$panorama_address" -panorama-admin-username="$panorama_admin_username" -panorama-admin-password="$panorama_admin_password"
  return $?
}

run
