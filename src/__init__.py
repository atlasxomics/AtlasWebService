from pathlib import Path 
import yaml
import logging
from logging.handlers import TimedRotatingFileHandler
import argparse

## Flask related
from flask import Flask

## App related
from src.storage import StorageAPI
from src.tasks import TaskAPI
from flask import render_template
    

## arguments

parser = argparse.ArgumentParser(prog="AtlasCloud")
parser.add_argument('--config',help='configuration file',default='config.yml',type=str)
args=parser.parse_args()

## App declaration
app=Flask(__name__)
# @app.route("/")
# def hello():
#     return render_template('index.html')
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
app.config['SUBMODULES']['StorageAPI']=StorageAPI(app)
