import boto3
import json
import os
import logging
import urllib.parse
from io import BytesIO
from pypdf import PdfReader

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"Environment: {os.environ}")
    logger.info(f"Event: {event}")
    
    try:
        # 1. Get bucket and fix the "+" filename issue
        bucket = event['Records'][0]['s3']['bucket']['name']
        raw_key = event['Records'][0]['s3']['object']['key']
        key = urllib.parse.unquote_plus(raw_key) 
        
        target_bucket = os.environ.get('TARGET_BUCKET', 'gl-inter-store-2026')
        s3_client = boto3.client('s3')

        # 2. Download and read PDF
        response = s3_client.get_object(Bucket=bucket, Key=key)
        pdf_bytes = response['Body'].read()
        
        reader = PdfReader(BytesIO(pdf_bytes))
        full_text = ""
        for page in reader.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n"

        # 3. Save as .txt
        output_key = key.lower().replace('.pdf', '.txt')
        s3_client.put_object(
            Bucket=target_bucket,
            Key=output_key,
            Body=full_text
        )
        
        logger.info(f"Success! Saved {output_key} to {target_bucket}")
        return {
            'statusCode': 200,
            'body': json.dumps('Execution is now complete')
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise e