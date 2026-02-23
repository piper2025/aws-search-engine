import boto3
import re
import requests
import math
from requests_aws4auth import AWS4Auth
import os

# 1. CONFIGURATION
region = 'us-east-1' 
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# 2. OPENSEARCH SETTINGS
host = 'https://vpc-my-search-domain-fm4k5yzopcnob2lrlneepiurzq.us-east-1.es.amazonaws.com'
index = 'lambda-index'
datatype = '_doc'
headers = { "Content-Type": "application/json" }

s3 = boto3.client('s3')

def listToString(s):
    str1 = ""
    for ele in s:
        # Decode bytes to string if necessary and add a newline for readability
        line = ele.decode('utf-8') if isinstance(ele, bytes) else ele
        str1 += line + "\n"
    return str1

def lambda_handler(event, context):
    for record in event['Records']:
        # Get source bucket and filename
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print(f"Processing file: {key} from bucket: {bucket}")

        # 3. READ THE FILE FROM S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        lines = body.splitlines()
        
        # OpenSearch URL
        url = host + '/' + index + '/' + datatype + '/' + key
        
        # Basic parsing (assumes first 3 lines are Title, Author, Date)
        title = lines[0].decode('utf-8') if len(lines) > 0 else "No Title"
        author = lines[1].decode('utf-8') if len(lines) > 1 else "Unknown"
        date = lines[2].decode('utf-8') if len(lines) > 2 else "Unknown"
        final_body_lines = lines[3:]
        
        # Convert the body lines into one big string
        full_text_content = listToString(final_body_lines)
        
        document = {
            "Title": title,
            "Author": author,
            "Date": date, 
            "Body": full_text_content
        }
        
        # 4. SEND TO OPENSEARCH
        print(f"Sending {key} to OpenSearch...")
        r = requests.post(url, auth=awsauth, json=document, headers=headers)
        print("OpenSearch Response:", r.text)
        
        # 5. SAVE RAW TEXT TO BACKUP BUCKET
        # This part ensures the file shows up in your 'search-text-rawtext' bucket
        try:
            raw_text_bucket = 'search-text-rawtext'
            # We save it as a .txt file
            output_key = key.replace('.pdf', '.txt') if key.endswith('.pdf') else key
            
            s3.put_object(
                Bucket=raw_text_bucket, 
                Key=output_key, 
                Body=full_text_content
            )
            print(f"Successfully saved processed text to {raw_text_bucket}/{output_key}")
        except Exception as e:
            print(f"Error saving to S3: {str(e)}")
        

        