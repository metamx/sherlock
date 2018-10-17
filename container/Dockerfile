FROM ubuntu:16.04
LABEL maintainer backend@metamarkets.com

RUN echo "Install curl & gettext" && \
    apt-get update && \
    apt-get install -y curl gettext

RUN echo "Installing Java 8" && \
    curl http://pkg.metamx-mgmt.com/java/jdk-8u191-linux-x64.tar.gz -o /tmp/jdk.tar.gz && \
    tar xf /tmp/jdk.tar.gz -C /tmp && \
    mv /tmp/jdk1.8* /jdk

RUN echo "Install python and dependencies" && \
    apt-get install -y python-pip python-dev python python-matplotlib

RUN echo "Installing pip dependencies" && \
    pip install --upgrade pip==9.0.3 && \
    pip install pystan && \
    pip install fbprophet

COPY container/app /app

COPY target/sherlock-1.0.12-mmx-SNAPSHOT-selfcontained.jar /app/lib/

WORKDIR /app

EXPOSE 9090

ENTRYPOINT /app/bin/entrypoint.sh
