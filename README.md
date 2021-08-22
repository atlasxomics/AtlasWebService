# AtlasWebService
A web service for automating metadata collection and bioinformatics analysis

### ATX-CLI

#### Login and get token
Login can generate the api token
```
$ atx-tools login 
$ export ATX_TOKEN=<token>
$ atx-tools whoami
```

Explicit way of supplying token in command line
```
$ atx-tools -a <token> COMMAND 
```

### Requirements
- >= python3.6
- flask
- flask_jwt_extended
- flask_cors
- boto3
- pyyaml
- warrant
- pymongo

### Database

- MongoDB
