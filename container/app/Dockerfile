FROM java:8-jre
LABEL maintainer backend@metamarkets.com

RUN echo "Install curl & gettext" && \
    apt-get update && \
    apt-get install -y gettext

COPY container/app /app

COPY target/sherlock-1.0.12-mmx-SNAPSHOT-selfcontained.jar /app/lib/

WORKDIR /app

EXPOSE 4080

ENTRYPOINT /app/bin/entrypoint.sh
