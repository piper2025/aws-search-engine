import boto3
import re
import requests
import math
from requests_aws4auth import AWS4Auth
import os

# 1. FIXED REGION
region = 'us-east-1' 
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# 2. FIXED HOST & INDEX
host = 'https://vpc-my-search-domain-fm4k5yzopcnob2lrlneepiurzq.us-east-1.es.amazonaws.com'
index = 'lambda-index'
datatype = '_doc'
headers = { "Content-Type": "application/json" }

s3 = boto3.client('s3')

def listToString(s):
    str1 = ""
    for ele in s:
        # Added a decode check to prevent crashing if the line is already a string
        str1 += ele.decode('utf-8') if isinstance(ele, bytes) else ele
    return str1

# 3. RENAMED TO lambda_handler
def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print(f"Processing file: {key} from bucket: {bucket}")

        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        lines = body.splitlines()
        
        # OpenSearch URL
        url = host + '/' + index + '/' + datatype + '/' + key
        
        # Basic parsing (assumes first 3 lines are Title, Author, Date)
        title = lines[0].decode('utf-8') if len(lines) > 0 else "No Title"
        author = lines[1].decode('utf-8') if len(lines) > 1 else "Unknown"
        date = lines[2].decode('utf-8') if len(lines) > 2 else "Unknown"
        final_body = lines[3:]
        
        document = {
            "Title": title,
            "Author": author,
            "Date": date, 
            "Body": listToString(final_body)
        }
        
        # 4. SEND TO OPENSEARCH
        r = requests.post(url, auth=awsauth, json=document, headers=headers)
        print("Response:", r.text)
        
        

        