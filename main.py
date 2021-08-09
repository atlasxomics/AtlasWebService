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

## Flask related
from flask import Flask

## App related
from src.auth import Auth 
from src.database import DynamoDB

## App declaration
app=Flask(__name__)

## loading configuration
config_filename="config.yml"
version_filename="version.yml"
config=yaml.safe_load(open(config_filename,'r'))    
version=yaml.safe_load(open(version_filename,'r'))
app.config.update(config)
app.config.update(version)

## logging
log_dir=Path(config['log_directory'])
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
app.config['SUBMODULES']['Database']=DynamoDB(app.config['SUBMODULES']['Auth'])

if __name__=="__main__": ## only for developpment. Production server needs to be wrapped by UWSGI like gateways
    app.run()




