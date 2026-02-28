import boto3
import json
import urllib.request
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# 1. CONFIGURATION
host = 'vpc-my-search-domain-fm4k5yzopcnob2lrlneepiurzq.us-east-1.es.amazonaws.com'
region = 'us-east-1'
index = 'lambda-index'

def lambda_handler(event, context):
    # 2. EXTRACT QUERY FROM API GATEWAY
    query_params = event.get('queryStringParameters') if event else None
    query_term = query_params.get('q', '*') if (query_params and query_params.get('q')) else '*'
    
    url = f"https://{host}/{index}/_search"
    
    # 3. BUILD SEARCH PAYLOAD
    payload_dict = {
        "size": 20,
        "query": {
            "multi_match": {
                "query": query_term,
                "fields": ["*"] 
            }
        },
        "highlight": {
            "fields": {
                "*": {}  # This enables highlighting for ALL fields
            },
            "pre_tags": ["<em>"],
            "post_tags": ["</em>"]
        }
    }
    payload = json.dumps(payload_dict).encode('utf-8')

    # 4. SIGN THE REQUEST
    session = boto3.Session()
    credentials = session.get_credentials()
    
    headers = {
        'host': host,
        'Content-Type': 'application/json'
    }

    aws_request = AWSRequest(
        method='POST',
        url=url,
        data=payload,
        headers=headers
    )
    
    SigV4Auth(credentials, 'es', region).add_auth(aws_request)
    
    # 5. EXECUTE THE SEARCH
    req = urllib.request.Request(
        url, 
        data=payload, 
        headers=dict(aws_request.headers), 
        method='POST'
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            res_body = response.read().decode('utf-8')
            
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*" 
                },
                "body": res_body
            }
            
    except urllib.error.HTTPError as e:
        error_msg = e.read().decode('utf-8')
        print(f"Search Service Error: {error_msg}")
        return {
            "statusCode": e.code,
            "body": json.dumps({"error": "Search service error", "details": error_msg})
        }
    except Exception as e:
        print(f"General Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"})
        }