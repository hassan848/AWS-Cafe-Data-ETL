import boto3
import csv
from io import StringIO
import json

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    print(event)
    # Get the bucket name and key for the uploaded file
    parsed_message = json.loads(event['Records'][0]['body'])
    source_bucket = parsed_message['Records'][0]['s3']['bucket']['name']
    key = parsed_message['Records'][0]['s3']['object']['key']
    # s3_info = event['Records'][0].get('s3', {})
    # source_bucket = s3_info.get('bucket', {}).get('name')
    #key = s3_info.get('object', {}).get('key')
    print(source_bucket, key)
    
    # Get the file object from S3
    file_obj = s3.get_object(Bucket=source_bucket, Key=key)
    file_content = file_obj['Body'].read().decode('utf-8')
    
    # Stores intended headers for the incoming csv
    headers = ['date', 'location', 'name', 'basket', 'total', 'payment_type', 'card_number']
    
    # Read the CSV data into a list of dictionaries
    #data = list(csv.DictReader(StringIO(file_content)))
    data = list(csv.DictReader(StringIO(file_content), fieldnames=headers))
    
    # Modify the data
    data = your_modification_function(data)
    
    # Convert the modified data back to CSV
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    modified_file_content = csv_buffer.getvalue()
    
    # Write the modified file to the new bucket
    destination_bucket = 'onedersdeployment'
    s3.put_object(Body=modified_file_content, Bucket=destination_bucket, Key=key)
    
    return {
        'statusCode': 200,
        'body': f'File {key} copied from {source_bucket} and cleaned to {destination_bucket} with modifications'
    }

def your_modification_function(data):
    for record in data:
        record['basket'] = record['basket'].split(', ')
        items = []
        for item in record['basket']:
            quantity = record['basket'].count(item)
            item += f' - {quantity}'
            if quantity > 1 and item in items: continue
            items.append(item)
            
        record['basket'] = items
        
        for i in range(len(record['basket'])):
            product_name, product_price, quantity = record['basket'][i].rsplit(' - ', 2)
                
            product_dict = {
                'product' : product_name,
                'price' : float(product_price),
                'quantity' : int(quantity)
            }
            record['basket'][i] = product_dict
            
    for row in data:
            del row["name"]
            del row["card_number"]

    return data