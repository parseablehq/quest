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

stream_name=$(head /dev/urandom | tr -dc a-z | head -c10)

run_smoke_test () {
  ./testcases/smoke_test.sh "$endpoint" "$stream_name" "$username" "$password"
  return $?
}

run_load_test () {
  ./testcases/load_test.sh "$endpoint" "$stream_name" "$username" "$password" "$schema_count"
  return $?
}

case "$mode" in
   "smoke") run_smoke_test 
   ;;
   "load") run_load_test 
   ;;
esac
