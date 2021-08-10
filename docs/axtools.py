import json,yaml
import argparse
from pathlib import Path 
import traceback,os,sys
import csv
import boto3

### commands 
def generate_template(payload):
    args=payload['command_args']
    filename=args.input_file
    data=json.load(open(filename,'r'))
    outobj={}
    for k0,v0 in data.items():
        attrs=[]
        if k0=='id':
            outobj['id']=v0
        if type(v0)==dict:
            print("{} : Count - {}".format(k0,len(v0.items())))
            for k,v in v0.items():
                tmp={"name": k, "value": v , "type" : str(type(v).__name__),"options":None, "description" : None}
                attrs.append(tmp)
            outobj[k0]=attrs
        if k0 in ['wetlab']:
            outfn=k0+".yml"
            yaml.dump(outobj[k0],open(outfn,'w'),indent=4)

    csv_files=['dbit_runs.csv','chips.csv','wafers.csv']
    for fn in csv_files:
        with open(fn,'r') as f:
            csv_reader=csv.DictReader(f,delimiter=',')
            dataset=[]
            outfilename=Path(fn).parent.joinpath(Path(fn).stem+".json")
            for r in csv_reader:
                dataset.append(r)
            json.dump(dataset,open(outfilename,'w'),indent=4)

    return 0

### upload data to dynamodb
def upload_data_from_json(payload):
    res=None
    args=payload['command_args']
    params={
        "input_file" : args.input_file,
        "config_file" : args.config
    }
    config=yaml.safe_load(open(params['config_file'],'r'))
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_DEFAULT_REGION']=config['AWS_DEFAULT_REGION']

    client=boto3.resource('dynamodb')
    print(client)

    return res 
### argument parser
def get_args():
    parser=argparse.ArgumentParser(prog='ax-converters',description='AtlasXomics development CLI tool',
                                    epilog='Written by SK Park, Copyright reserved by AtlasXomics, 2021')
        ## default arguments
    parser.add_argument('--host',default='https://ax.sentinel-holdings.com/test',type=str,help='default url including port')
    parser.add_argument('-a','--access-token',default=None,type=str,help="Access token without JWT prefix")
    parser.add_argument('-u','--login-username',default=None,type=str,help='Login username')
    parser.add_argument('-p','--login-password',default=None,type=str,help='Login password')
    parser.add_argument('-c','--config',help='AWS configuration file',type=str,default='../config.yml')

    subparsers=parser.add_subparsers(help="Commands")

        ## generate data templates command
    parser_generate_template=subparsers.add_parser('generate_template',help='Returns the user informations')
    parser_generate_template.add_argument('input_file',default=None,type=str,help='Input file (CSV)')
    parser_generate_template.set_defaults(func=generate_template)
   
        ## upload json to dynamodb command
    parser_upload_data_from_json=subparsers.add_parser('upload_data_from_json',help='Returns the user informations')
    parser_upload_data_from_json.add_argument('input_file',default=None,type=str,help='Input file (JSON)')
    parser_upload_data_from_json.add_argument('-t','--table',default=None,type=str,help='Table name')
    parser_upload_data_from_json.set_defaults(func=upload_data_from_json)

    if len(sys.argv)==1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args=parser.parse_args()
    return args 


### main entry
if __name__=="__main__":

    args=get_args()
    payload={}
    payload["command_args"]=args

    try:
        result=args.func(payload)
        print(result)
        exit(0)
    except Exception as e:
        msg=traceback.format_exc()
        print(msg)
        exit(-1)
    finally:
        pass
