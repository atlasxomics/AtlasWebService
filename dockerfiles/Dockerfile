FROM ubuntu:20.04
WORKDIR /
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y &&\
    apt-get install build-essential python3-pip python3-venv libgtk2.0-dev -y &&\
    pip3 install uwsgi flask flask_restful flask-cors pymongo requests boto3 requests celery celery[redis] pika redis &&\
    pip3 install flask_jwt_extended uwsgi &&\
    pip3 install pyyaml warrant numpy==1.23.3 scanpy leidenalg
RUN pip3 install opencv-python
RUN apt-get install libgl1-mesa-glx -y
RUN pip3 install scipy
RUN pip3 install SQLAlchemy==1.4.46
RUN pip3 install PyMySQL
RUN pip3 install openpyxl
RUN pip3 install pytest

