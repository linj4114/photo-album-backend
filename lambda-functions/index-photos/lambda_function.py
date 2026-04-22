import boto3
import json
import urllib.parse
from datetime import datetime
import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth

# Configuration
REGION = 'us-east-1'
ES_ENDPOINT = 'https://search-photos-o2rzcc7mruqikzvgt4hov3bcoa.us-east-1.es.amazonaws.com'
ES_HOST = 'search-photos-o2rzcc7mruqikzvgt4hov3bcoa.us-east-1.es.amazonaws.com'
ES_INDEX = 'photos'

def get_es_auth():
    credentials = boto3.Session().get_credentials()
    credentials = credentials.get_frozen_credentials()
    return AWSRequestsAuth(
        aws_access_key=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_token=credentials.token,
        aws_host=ES_HOST,
        aws_region=REGION,
        aws_service='es'
    )

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    rekognition = boto3.client('rekognition', region_name=REGION)
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    print(f"Processing photo: {key} from bucket: {bucket}")
    
    rek_response = rekognition.detect_labels(
        Image={'S3Object': {'Bucket': bucket, 'Name': key}},
        MaxLabels=10,
        MinConfidence=70
    )
    
    labels = [label['Name'].lower() for label in rek_response['Labels']]
    print(f"Rekognition labels: {labels}")
    
    head = s3.head_object(Bucket=bucket, Key=key)
    metadata = head.get('Metadata', {})
    print(f"All S3 metadata: {metadata}")
    custom_labels_raw = metadata.get('customlabels', '')
    
    if custom_labels_raw:
        custom_labels = [l.strip().lower() for l in custom_labels_raw.split(',')]
        labels.extend(custom_labels)
        print(f"Custom labels added: {custom_labels}")
    
    document = {
        'objectKey': key,
        'bucket': bucket,
        'createdTimestamp': datetime.now().isoformat(),
        'labels': labels
    }
    
    print(f"Document to index: {json.dumps(document)}")
    
    url = f"{ES_ENDPOINT}/{ES_INDEX}/_doc"
    headers = {"Content-Type": "application/json"}
    auth = get_es_auth()
    
    response = requests.post(url, auth=auth, json=document, headers=headers)
    print(f"ElasticSearch response: {response.status_code} - {response.text}")
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to index photo: {response.text}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Photo indexed successfully')
    }