#!/bin/bash

while ! nc -z $ES_HOST $ES_PORT; do
    sleep 1
done

curl -XPUT $ES_HOST:$ES_PORT/$ES_INDEX -H 'Content-Type: application/json' -d @config/es_index.json
python app/main.py