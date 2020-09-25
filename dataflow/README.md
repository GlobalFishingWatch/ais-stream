# Dataflow AIS Stream Decoder

Tool for running ais-tools AIVDM/NMEA decoder in dataflow

## Setup
```console
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt 
pip install .

cp config-template.sh config.sh
nano config.sh
```

## Authentication
For running from a desktop, you need to set up auth.  
https://cloud.google.com/docs/authentication/getting-started

One way is to download a service account key and set GOOGLE_APPLICATION_CREDENTIALS

Make sure the service account has these roles
* BigQueryData Editor
* Dataflow Admin
* Pub/Sub Editor
* Storage Object Admin

```
export GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
```

## Execution
```console
./run.sh
```

## Developing
```console
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt 
pip install -e .\[dev\]
```



