#!/bin/sh

echo "Starting http-prophet..."
/usr/bin/python /app/http-prophet.py  &

envsubst < /app/conf/sherlock.properties.template > /app/conf/sherlock.properties

/jdk/bin/java $(cat conf/jvm.config | xargs) \
    -cp $(ls lib/sherlock-*-selfcontained.jar | xargs | tr ' ' ':') \
    com.yahoo.sherlock.App --config /app/conf/sherlock.properties
