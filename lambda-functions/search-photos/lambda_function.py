import boto3
import json
import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth

REGION = 'us-east-1'
ES_ENDPOINT = 'https://search-photos-o2rzcc7mruqikzvgt4hov3bcoa.us-east-1.es.amazonaws.com'
ES_HOST = 'search-photos-o2rzcc7mruqikzvgt4hov3bcoa.us-east-1.es.amazonaws.com'
ES_INDEX = 'photos'
LEX_BOT_ID = '4B4TQSOLV0'
LEX_BOT_ALIAS_ID = 'TSTALIASID'

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

def get_keywords_from_lex(query):
    client = boto3.client('lexv2-runtime', region_name=REGION)
    
    response = client.recognize_text(
        botId=LEX_BOT_ID,
        botAliasId=LEX_BOT_ALIAS_ID,
        localeId='en_US',
        sessionId='search-session-123',
        text=query
    )
    
    print(f"Lex response: {json.dumps(response, default=str)}")
    
    keywords = []
    slots = response.get('sessionState', {}).get('intent', {}).get('slots', {})
    
    for slot_name, slot_value in slots.items():
        if slot_value and slot_value.get('value', {}).get('interpretedValue'):
            val = slot_value['value']['interpretedValue'].lower()
            keywords.append(val)
    
    return keywords

def search_opensearch(keywords):
    auth = get_es_auth()
    headers = {"Content-Type": "application/json"}
    
    should_clauses = []
    for keyword in keywords:
        should_clauses.append({"match": {"labels": {"query": keyword, "fuzziness": "AUTO"}}})
        if keyword.endswith('s'):
            should_clauses.append({"match": {"labels": keyword[:-1]}})
        else:
            should_clauses.append({"match": {"labels": keyword + 's'}})
        
    query = {
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1
            }
        }
    }
    
    url = f"{ES_ENDPOINT}/{ES_INDEX}/_search"
    response = requests.get(url, auth=auth, headers=headers, json=query)
    
    print(f"OpenSearch response: {response.status_code} - {response.text}")
    
    results = []
    if response.status_code == 200:
        hits = response.json().get('hits', {}).get('hits', [])
        for hit in hits:
            source = hit['_source']
            results.append({
                'url': f"https://{source['bucket']}.s3.amazonaws.com/{source['objectKey']}",
                'labels': source['labels']
            })
    
    return results

def lambda_handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    query = event.get('queryStringParameters', {}).get('q', '')
    
    if not query:
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
            },
            'body': json.dumps({'results': []})
        }
    
    print(f"Search query: {query}")
    
    keywords = get_keywords_from_lex(query)
    print(f"Keywords from Lex: {keywords}")
    
    if not keywords:
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
            },
            'body': json.dumps({'results': []})
        }
    
    results = search_opensearch(keywords)
    print(f"Search results: {results}")
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        'body': json.dumps({'results': results})
    }