import json,yaml
import argparse
import time
from pathlib import Path 
import uuid
import traceback,os,sys
import csv
import requests
import math as m
from functools import wraps
from getpass import getpass


def get_uuid():
    return str(uuid.uuid4())

def get_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  

def get_timestamp_iso():
    return datetime.datetime.now().isoformat() 

def error_message(msg,error_code=404):
    res={"type":"error","message":msg, "status_code":error_code}
    return error_code,res

def sanity_check(payload,keyarray):
    for k in keyarray:
        if not (k in payload):
            return False
    return True

def download_from_link(url,params,headers,output_filename,n_chunk=1): ### download by chunks
    current_dir=Path(__file__).parent
    r = requests.get(url, params=params,headers=headers,stream=True)
    block_size = 1024
    file_size = int(r.headers.get('Content-Length', None))
    print("File size : {}".format(file_size))
    num_bars = m.ceil(file_size / (n_chunk * block_size))
    bt=time.time()
    with open(output_filename, 'wb') as f:
        for i, chunk in enumerate(r.iter_content(chunk_size=n_chunk * block_size)):
            f.write(chunk)
            et=time.time()-bt
            trspeed=(i+1)/et
            print("\rDownloading {:.1f}mb/{:.1f}mb of {} ({:.1f}kb/s)".format((i+1)/block_size,file_size/(block_size**2),Path(output_filename).name,trspeed),end='\r')
            #time.sleep(0.1)
    print()
    return output_filename

## decorator
def token_required(func):
    @wraps(func)
    def wrapper(*args,**kwargs):
        username=None 
        password=None
        if args[0]['command_args'].access_token is None:
            if 'ATX_TOKEN' in os.environ:
                access_token=os.environ['ATX_TOKEN']
                args[0]['access_token']='JWT ' + access_token
                return func(*args,**kwargs)
            else:
                sc,access_token=login(args[0])
                args[0]['access_token']='JWT ' + access_token
                return func(*args,**kwargs)
        else:
            access_token=args[0]['command_args'].access_token
            args[0]['access_token']='JWT ' + access_token
            return func(*args,**kwargs)
    return wrapper

###### AUTH commands 
def login(payload):

    if payload['command_args'].login_username is not None :
        username=payload['command_args'].login_username
        if payload['command_args'].login_password is not None :
            password=payload['command_args'].login_password
        else:
            password=getpass("Password : ")
    else:
        username=input("Username : ")
        password=getpass("Password : ")

    auth_info={"username":username,"password":password}
    headers={"Content-Type":"application/json"}
    uri=payload["command_args"].host+'/api/v1/auth/login'
    res=requests.post(uri, data=json.dumps(auth_info),headers=headers)
    if res.status_code==200:
        payload['access_token']='JWT '+json.loads(res.content)['access_token']
        at=payload['access_token'].split()[1]
        return 200, at
    else:
        msg="Error in signin"
        return error_message(msg,401)

def change_password(payload):
    sc,at=login(payload)
    if sc!=200:
        return error_message("Error in signing in : {}".format(str(at)),sc)
    token="JWT "+at
    uri=payload["command_args"].host+'/api/v1/auth/changepassword'
    headers={"Content-Type":"application/json","Authorization":token}

    old_password=getpass("Old password : ")
    new_password=getpass("New password : ")
    new_password_confirm=getpass("Confirm new password : ")
    if new_password!=new_password_confirm:
        return error_message("New passwords don't match, pls try again",500)
    auth_info={
        "old_password": old_password,
        "new_password": new_password
    }
    res=requests.put(uri,data=json.dumps(auth_info),headers=headers)
    if res.status_code==200:
        return 200, str(res.content)
    else:
        return error_message("Error in changing password", res.status_code)

@token_required
def whoami(payload):
    uri=payload["command_args"].host+'/api/v1/auth/whoami'
    token=payload['access_token']
    headers={"Content-Type":"application/json","Authorization":token}
    res=requests.get(uri,headers=headers)
    if res.status_code==200:
        return 200, str(res.content)
    else:
        return error_message("Error in whoami", res.status_code)

@token_required
def reset_password(payload):
    uri=payload["command_args"].host+'/api/v1/auth/resetpassword'
    token=payload['access_token']
    user_info={}
    user_info["username"]=payload['command_args'].username
    user_info["password"]=payload['command_args'].password
    headers={"Content-Type":"application/json","Authorization":token}
    res=requests.put(uri,data=json.dumps(user_info),headers=headers)
    if res.status_code==200:
        return 200, str(res.content)
    else:
        return error_message("Error in resetting password", res.status_code)

@token_required
def register_user(payload):
    uri=payload["command_args"].host+'/api/v1/auth/user'
    token=payload['access_token']
    headers={"Content-Type":"application/json","Authorization":token}
    user_info={}
    user_info["username"]=payload['command_args'].username
    user_info["password"]=payload['command_args'].password
    user_info["name"]=payload['command_args'].name
    user_info["email"]=payload['command_args'].email

    res=requests.post(uri,data=json.dumps(user_info),headers=headers)
    if res.status_code==200:
        return 200, str(res.content)
    else:
        return error_message("Error in Registering a user", res.status_code)

@token_required
def confirm_user(payload):
    uri=payload["command_args"].host+'/api/v1/auth/confirm/{}'.format(payload['command_args'].username)
    token=payload['access_token']
    headers={"Content-Type":"application/json","Authorization":token}
    res=requests.put(uri,headers=headers)
    if res.status_code==200:
        return 200, str(res.content)
    else:
        return error_message("Error in Registering a user", res.status_code)

@token_required
def assign_group(payload):
    username=payload['command_args'].username
    groupname=payload['command_args'].group
    uri=payload["command_args"].host+'/api/v1/auth/group/{}/{}'.format(username,groupname)
    token=payload['access_token']
    headers={"Content-Type":"application/json","Authorization":token}
    res=requests.put(uri,headers=headers)
    if res.status_code==200:
        return 200, str(res.content)
    else:
        return error_message("Error in assigning  group {} to the user {}".format(groupname,username), res.content)

@token_required
def find_user(payload):
    token=payload['access_token']
    uri=payload["command_args"].host+'/api/v1/auth/user'
    if payload['command_args'].username is not None:
        uri+="/{}".format(payload['command_args'].username)

    headers={"Content-Type":"application/json","Authorization":token}
    params={}
    res=requests.get(uri,headers=headers,params=params)
    if res.status_code==200:
        return 200, json.dumps(json.loads(res.content),indent=4)
    else:
        return error_message("Error in Finding a user", res.status_code)

@token_required
def remove_user(payload):
    username=payload['command_args'].username
    uri=payload["command_args"].host+'/api/v1/auth/user/{}'.format(username)
    token=payload['access_token']
    headers={"Content-Type":"application/json","Authorization":token}
    res=requests.delete(uri,headers=headers)
    if res.status_code==200:
        return 200, str(res.content)
    else:
        return error_message("Error in Removing a user", res.status_code)

### upload data to api
@token_required
def upload_dataset(payload):
    token=payload['access_token']
    filename=payload['command_args'].input_file 
    table=payload['command_args'].table 
    uri=payload["command_args"].host+'/api/v1/dataset/{}'.format(table)
    headers={"Content-Type":"application/json","Authorization":token}
    data=yaml.safe_load(open(filename,'r'))
    for idx,d in enumerate(data):
        if '_id' not in d:
            data[idx]['_id']=get_uuid()
    res=requests.post(uri,data=json.dumps(data),headers=headers)
    if res.status_code==200:
        return 200, str(res.content)
    else:
        return error_message("Error in uploading", res.status_code)    

@token_required
def generate_s3_dataset(payload):
    token=payload['access_token']
    bucket_name=payload['command_args'].bucket_name
    root_directory=payload['command_args'].root_directory
    out_filename=payload['command_args'].output
    uri=payload["command_args"].host+'/api/v1/storage/list'
    headers={"Content-Type":"application/json","Authorization":token}
    params={
        'path' : root_directory,
        'bucket': bucket_name
    }
    res=requests.get(uri,params=params,headers=headers)
    object_list=json.loads(res.content)
    object_list=list(map(lambda fp: fp.split('/')[1:],object_list))
    output=[]
    keys=list(filter(lambda y: len(y)>0 ,set([x[0].split('.')[0] for x in object_list])))
    for k in keys:
        files=list(map(lambda z: "/".join(z) ,filter(lambda y: y[0].split('.')[0]==k,object_list)))
        temp_obj={
            "id":k,
            "metadata":{},
            "files":{
                "bucket": bucket_name,
                "root": root_directory,
                "images":{},
                "data":{},
                "meta":{},
                "other":{}
            }
        }
        for fn in files:
            category='other'
            if '.png' in fn.lower() or '.tiff' in fn.lower() or '.jpg' in fn.lower() or '.jpeg' in fn.lower():
                category='images'
            elif '.json' in fn.lower() or '.yml' in fn.lower():
                category='meta'
            elif '.csv' in fn.lower() or '.tsv' in fn.lower() or '.mtx' in fn.lower() or '.stat' in fn.lower():
                category='data'
            else:
                category='other'
            temp_obj['files'][category][fn.split('/')[-1].split('.')[0]]=fn
            ## load metadata using api
            if 'metadata.json' in fn.lower():
                meta_filename="{}/{}".format(root_directory,fn)
                print(meta_filename)
                temp_filename="temp.json"
                uri=payload['command_args'].host+'/api/v1/storage'
                params={'bucket':bucket_name,'filename':meta_filename}
                download_from_link(uri,params,headers,temp_filename)
                temp_obj['metadata']=json.load(open(temp_filename,'r'))
        del temp_obj['files']['other']
        output.append(temp_obj)
    os.remove('temp.json')
    with open(out_filename,'w') as f:
        yaml.dump(output,f)
    if res.status_code==200:
        return 200, ""#str(res.content)
    else:
        return error_message("Error in parsing", res.status_code)    
  

### utilities

def make_dataset_from_csv(payload):
    filename=payload['command_args'].input_file 
    output_filename=payload['command_args'].output 
    no_id=payload['command_args'].no_id
    keys=payload['command_args'].keys 
    dataset=[]
    def shorthand(s): #make name to shorthand variable
        s=s.lower()
        s=s.replace('\ufeff','')
        s=s.replace(' ','_')
        s=s.replace('(','_')
        s=s.replace(')','_')
        s=s.replace('__','_')
        s=s.replace('?','')
        return s 

    with open(filename,'r') as f:
        csv_reader=csv.DictReader(f,delimiter=',')    
        for r in csv_reader:
            processed_row=[]
            for k,v in r.items():
                try:
                    processed_row.append({
                        "name":shorthand(k),
                        "caption":k,
                        "type": int.__name__,
                        "value": int(v) })
                except:
                    try:
                        processed_row.append({
                        "name":shorthand(k),
                        "caption":k,
                        "type": float.__name__,
                        "value": float(v) })
                    except:
                        processed_row.append({
                        "name":shorthand(k),
                        "caption":k,
                        "type": str.__name__,
                        "value": v })
                if v=="" : 
                    processed_row.append({
                        "name":shorthand(k),
                        "caption":k,
                        "type": str.__name__,
                        "value": None })
            final_row={}
            for pr in processed_row:
                final_row[pr['name']]=pr['value']
            final_row['_id']=get_uuid()
            # print(final_row)
            # exit()
            pass_this=False
            for k in keys:
                if final_row[k] is None or final_row[k]=="":
                    pass_this=True
                    break
            if pass_this:
                continue
            dataset.append(final_row)
        json.dump(dataset,open(output_filename,'w'),indent=4)
    return 200, json.dumps(dataset)
### argument parser set up
def get_args():
    current_dir=Path(__file__).parent
    parser=argparse.ArgumentParser(prog="atx-tools",description="ATX Cloud API tools",
                                   epilog="Written by SK Park , AtlasXomics all rights reserved. 2021")
    
### AUTH  
    ## default arguments
    parser.add_argument('--host',default='http://34.200.77.43:5001',type=str,help='default url including port')
    parser.add_argument('-a','--access-token',default=None,type=str,help="Access token without JWT prefix")
    parser.add_argument('-u','--login-username',default=None,type=str,help='Login username')
    parser.add_argument('-p','--login-password',default=None,type=str,help='Login password')

    subparsers=parser.add_subparsers(help="Commands")
    
    ## login command
    parser_login=subparsers.add_parser('login',help='Returns access code')
    parser_login.set_defaults(func=login)

    ## whoami command
    parser_whoami=subparsers.add_parser('whoami',help='Returns the user information')
    parser_whoami.set_defaults(func=whoami)

    ## find_user command
    parser_find_users=subparsers.add_parser('find_user',help='Returns the user informations (admin)')
    parser_find_users.add_argument('--username',default=None,type=str,required=False,help='Existing Username')
    parser_find_users.set_defaults(func=find_user)

    ## register_user command
    parser_register_user=subparsers.add_parser('register_user',help='Register a user (admin)')
    parser_register_user.add_argument('--username',type=str,required=True,help='New username')
    parser_register_user.add_argument('--password',type=str,required=True,help='New password for the user')
    parser_register_user.add_argument('--name',type=str,required=True,help='Name of the user')
    parser_register_user.add_argument('--email',type=str,required=True,help='Email address of the user')
    parser_register_user.set_defaults(func=register_user)

    ## confirm_user command
    parser_confirm_user=subparsers.add_parser('confirm_user',help='Confirm a user (admin)')
    parser_confirm_user.add_argument('--username',type=str,required=True,help='Username to confirm')
    parser_confirm_user.set_defaults(func=confirm_user)

    ## assign_group command
    parser_assign_group=subparsers.add_parser('assign_group',help='Assign a user to the group (admin)')
    parser_assign_group.add_argument('--username',type=str,required=True,help='Username to assign to be in the group')
    parser_assign_group.add_argument('--group',type=str,required=True,help='Groupname (admin | enduser) to assign')
    parser_assign_group.set_defaults(func=assign_group)

    ## change_password command
    parser_change_password=subparsers.add_parser('change_password',help='Change password')
    parser_change_password.set_defaults(func=change_password)

    ## reset_password command
    parser_resetpassword=subparsers.add_parser('reset_password',help='Reset password for a user (admin)')
    parser_resetpassword.add_argument('--username',type=str,required=True,help='Username to reset the password')
    parser_resetpassword.add_argument('--password',type=str,required=True,help='New password for the user')
    parser_resetpassword.set_defaults(func=reset_password)

    ## remove_user command
    parser_remove_user=subparsers.add_parser('remove_user',help='Remove a user (admin)')
    parser_remove_user.add_argument('--username',type=str,required=True,help='Username to remove')
    parser_remove_user.set_defaults(func=remove_user)

## DATASET API
    ## upload dataset
    parser_upload_dataset=subparsers.add_parser('upload_dataset',help='Upload dataset (admin)')
    parser_upload_dataset.add_argument('input_file',type=str,help='Input dataset file (.json)')
    parser_upload_dataset.add_argument('-t','--table',type=str,required=True,help='Output table (wafers | chips | dbits)')
    parser_upload_dataset.set_defaults(func=upload_dataset)      

    ## parse_s3 and generate database entry storing file information
    parser_generate_s3_dataset=subparsers.add_parser('generate_s3_dataset',help='Generate DB entries from S3 (admin)')
    parser_generate_s3_dataset.add_argument('bucket_name',type=str,help='S3 bucket name')
    parser_generate_s3_dataset.add_argument('-d','--root-directory',type=str,required=True,help='Root directory to parse')
    parser_generate_s3_dataset.add_argument('-o','--output',type=str,default='output.yml',help='output filename')
    parser_generate_s3_dataset.set_defaults(func=generate_s3_dataset)

## UTILITIES
    parser_make_dataset_from_csv=subparsers.add_parser('make_dataset_from_csv',help='Make a dataset (admin)')
    parser_make_dataset_from_csv.add_argument('input_file',type=str,help='Input file (csv)')
    parser_make_dataset_from_csv.add_argument('-o','--output',type=str,default='output.json',help='Output file (.json)')
    parser_make_dataset_from_csv.add_argument('-k','--keys',nargs='+',required=True,help='Keys to have values')
    parser_make_dataset_from_csv.add_argument('--no-id',default=False,help='If there is no id, id will be generated automatically',action='store_true')
    parser_make_dataset_from_csv.set_defaults(func=make_dataset_from_csv)    

    ## if no parameter is furnished, exit with printing help
    if len(sys.argv)==1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args=parser.parse_args()
    return args 

## main entry
if __name__=='__main__':
    args=get_args()
    payload={}
    payload["command_args"]=args

    try:
        print("Host : {}".format(args.host))
        sc,result=args.func(payload)
        if sc==200:
            print(result)
        else:
            print("[{}] {}".format(str(sc),str(result)))
        exit(0)
    except Exception as e:
        msg=traceback.format_exc()
        print("{} {}".format(str(e),msg))
        exit(-1)
    finally:
        pass
