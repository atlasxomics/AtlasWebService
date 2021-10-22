FROM alpine:3.14
WORKDIR /
ARG DEBIAN_FRONTEND=noninteractive
RUN apk add python3 py3-pip python3-dev build-base linux-headers pcre-dev &&\
    pip3 install uwsgi flask flask_restful flask-cors pymongo requests boto3 &&\
    pip3 install flask_jwt_extended uwsgi pyyaml warrant scanpy numpy
EXPOSE 5000
EXPOSE 5001

