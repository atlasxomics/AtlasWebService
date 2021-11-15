##################################################################################
### Module : main.py
### Description : AtlasXomics Data API Entrypoint
###
###
###
### Written by : scalphunter@gmail.com ,  2021/08/04
### Copyrighted reserved by AtlasXomics
##################################################################################

## System related
import os,traceback
from pathlib import Path 
import yaml
import logging
from logging.handlers import TimedRotatingFileHandler
import argparse

## Flask related
from flask import Flask

## App related
from src.auth import Auth 
from src.database import MongoDB as Database
from src.storage import StorageAPI
from src.dataset import DatasetAPI
from src.genes import GeneAPI
from src.tasks import TaskAPI
## arguments

parser = argparse.ArgumentParser(prog="AtlasCloud")
parser.add_argument('--config',help='configuration file',default='config.yml',type=str)
args=parser.parse_args()

## App declaration
app=Flask(__name__)

## loading configuration
config_filename=args.config
print("Config Filename is : {}".format(config_filename))
version_filename="version.yml"
config=yaml.safe_load(open(config_filename,'r'))    
version=yaml.safe_load(open(version_filename,'r'))
app.config.update(config)
app.config.update(version)

## logging
log_dir=Path(config['LOG_DIRECTORY'])
log_dir.mkdir(parents=True,exist_ok=True)
formatter = logging.Formatter('[%(asctime)s]  %(levelname)s :: %(message)s :: {%(pathname)s:%(lineno)d}')
logpath=log_dir.joinpath('ax-api')
logging.root.setLevel(logging.NOTSET)
handler=TimedRotatingFileHandler(logpath,when='D',interval=1,backupCount=100)
handler.setFormatter(formatter)
handler.setLevel(logging.NOTSET)
app.logger.addHandler(handler)
app.logger.debug("Application is launched")

## app config

app.config['APP_VERSION']=version
app.config['SUBMODULES']={}
app.config['SUBMODULES']['Auth']=Auth(app)
app.config['SUBMODULES']['Database']=Database(auth=app.config['SUBMODULES']['Auth'])
app.config['SUBMODULES']['StorageAPI']=StorageAPI(  auth=app.config['SUBMODULES']['Auth'],
                                                    datastore=app.config['SUBMODULES']['Database'])
app.config['SUBMODULES']['DatasetAPI']=DatasetAPI(  auth=app.config['SUBMODULES']['Auth'],
                                                    datastore=app.config['SUBMODULES']['Database'])
app.config['SUBMODULES']['GeneAPI']=GeneAPI(  auth=app.config['SUBMODULES']['Auth'],
                                                    datastore=app.config['SUBMODULES']['Database'])
app.config['SUBMODULES']['TaskAPI']=TaskAPI(  auth=app.config['SUBMODULES']['Auth'],
                                                    datastore=app.config['SUBMODULES']['Database'])

if __name__=="__main__": ## only for developpment. Production server needs to be wrapped by UWSGI like gateways
    app.run(host="0.0.0.0")




