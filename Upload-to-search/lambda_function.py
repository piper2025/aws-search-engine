import boto3
import re
import requests
import math
from requests_aws4auth import AWS4Auth
import os
import urllib.parse  # Crucial for handling spaces/plus signs in filenames

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
        try:
            # Decode bytes to string and clean up whitespace
            line = ele.decode('utf-8').strip() if isinstance(ele, bytes) else str(ele).strip()
            if line:
                str1 += line + " "
        except Exception:
            continue
    return str1

def lambda_handler(event, context):
    for record in event['Records']:
        # 3. GET AND DECODE FILENAME
        bucket = record['s3']['bucket']['name']
        raw_key = record['s3']['object']['key']
        # This converts 'id+p.txt' back to 'id p.txt' so S3 can find it
        key = urllib.parse.unquote_plus(raw_key)

        print(f"Processing file: {key} from bucket: {bucket}")

        try:
            # 4. READ THE FILE FROM S3
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj['Body'].read()
            lines = body.splitlines()
            
            # OpenSearch URL (using the cleaned 'key' as the ID)
            url = f"{host}/{index}/{datatype}/{urllib.parse.quote(key)}"
            
            # Basic parsing
            title = lines[0].decode('utf-8') if len(lines) > 0 else "No Title"
            author = lines[1].decode('utf-8') if len(lines) > 1 else "Unknown"
            date = lines[2].decode('utf-8') if len(lines) > 2 else "Unknown"
            final_body_lines = lines[3:]
            
            full_text_content = listToString(final_body_lines)
            
            document = {
                "Title": title,
                "Author": author,
                "Date": date, 
                "Body": full_text_content
            }
            
            # 5. SEND TO OPENSEARCH
            print(f"Sending {key} to OpenSearch...")
            r = requests.post(url, auth=awsauth, json=document, headers=headers)
            print("OpenSearch Response:", r.text)
            
            # 6. SAVE RAW TEXT TO BACKUP BUCKET
            raw_text_bucket = 'search-text-rawtext'
            output_key = key.replace('.pdf', '.txt') if key.endswith('.pdf') else key
            
            s3.put_object(
                Bucket=raw_text_bucket, 
                Key=output_key, 
                Body=full_text_content
            )
            print(f"Successfully saved to {raw_text_bucket}/{output_key}")

        except Exception as e:
            print(f"Error processing {key}: {str(e)}")
            raise e

        