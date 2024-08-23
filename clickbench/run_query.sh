#!/bin/bash

QUERY_NUM=1
cat 'queries.sql' | while read -r QUERY; do

    JSON=$(jq -n --arg query "$QUERY" \
        '{query: $query, startTime: "2024-08-22T00:00:00.000Z", endTime: "2024-08-22T23:00:00.000Z"}')
    start_time=$(date +%s%3N)

    ES_RSP= curl  -H "Content-Type: application/json" -k -XPOST -u "admin:admin" "http://3.140.239.140:8000/api/v1/query" --data "${JSON}"

    end_time=$(date +%s%3N)
    elapsed_time=$((end_time - start_time))

    echo "${QUERY_NUM},${elapsed_time}" >> result.csv

    QUERY_NUM=$((QUERY_NUM + 1))

done;
