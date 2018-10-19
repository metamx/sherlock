FROM python:2.7
LABEL maintainer backend@metamarkets.com

#RUN echo "Install gettext" && \
#    apt-get update && \
#    apt-get install -y gettext

RUN echo "Install python and dependencies" && \
    apt-get update && \
    apt-get install -y python-matplotlib

RUN echo "Installing pip dependencies" && \
    pip install pystan && \
    pip install fbprophet

COPY container/prophet/http-prophet.py /app/

WORKDIR /app

EXPOSE 9090

ENTRYPOINT python /app/http-prophet.py
