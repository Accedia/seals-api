import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr

def get(event, context):
    # TODO implementa
    
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table('beaches')
    
    response = table.query(
        KeyConditionExpression=Key('beach_name').eq('Созопол'),
        Limit=1,
        ScanIndexForward=False
    )
    items = response['Items']
    print(items)
    
    return {
        'statusCode': 200,
        'body': json.dumps(items)
    }