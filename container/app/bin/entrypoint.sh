#!/bin/sh

envsubst < /app/conf/sherlock.properties.template > /app/conf/sherlock.properties

cat /app/conf/sherlock.properties

java $(cat conf/jvm.config | xargs) \
    -cp $(ls lib/sherlock-*-selfcontained.jar | xargs | tr ' ' ':') \
    com.yahoo.sherlock.App --config /app/conf/sherlock.properties
