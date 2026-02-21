import boto3
import json
import os
import logging
import urllib.parse
from io import BytesIO

# Robust Library Import
try:
    from pypdf import PdfReader
except ImportError:
    import pypdf
    PdfReader = pypdf.PdfReader

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"Event received: {json.dumps(event)}")
    try:
        # Get bucket and key
        bucket = event['Records'][0]['s3']['bucket']['name']
        raw_key = event['Records'][0]['s3']['object']['key']
        
        # CRITICAL FIX: Convert "ID+Card.pdf" back to "ID Card.pdf"
        key = urllib.parse.unquote_plus(raw_key)
        logger.info(f"Processing file: {key} from bucket: {bucket}")
        
        target_bucket = os.environ.get('TARGET_BUCKET', 'gl-inter-store-2026')
        s3_client = boto3.client('s3')

        # Download PDF
        response = s3_client.get_object(Bucket=bucket, Key=key)
        pdf_bytes = response['Body'].read()
        
        # Extract Text
        reader = PdfReader(BytesIO(pdf_bytes))
        full_text = ""
        for page in reader.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n"

        # Define Output Name
        output_key = key.replace('.pdf', '.txt').replace('.PDF', '.txt')
        
        # Upload Text
        s3_client.put_object(
            Bucket=target_bucket,
            Key=output_key,
            Body=full_text
        )
        logger.info(f"Successfully saved {output_key} to {target_bucket}")
        return {'statusCode': 200, 'body': 'Conversion Successful'}
        
    except Exception as e:
        logger.error(f"FATAL ERROR: {str(e)}")
        raise e