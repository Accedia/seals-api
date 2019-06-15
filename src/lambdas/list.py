from pymongo import MongoClient
from bson import json_util
import boto3
import json
import urllib.parse

def list_all(event, context):
    secret_name = "MONGO_URI"
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )

    secret_value = json.loads(get_secret_value_response['SecretString'])

    username = urllib.parse.quote_plus(secret_value['username'])
    password = urllib.parse.quote_plus(secret_value['password'])
    host = secret_value['host']

    mongo_client = MongoClient('mongodb://%s:%s@%s:27017' % (username, password, host))

    db = mongo_client.seals
    items = list(db.beaches.aggregate([
        {
            "$sort": {
                "measurement_date": -1
            }
        },
        {
            "$group": {
                "_id": "$point",
                "name": {
                    "$first": '$name'
                },
                "coord_x": {
                    "$first": "$coord_x"
                },
                "coord_y": {
                    "$first": "$coord_y"
                },
                "intestinal_enterococci": {
                    "$first": "$intestinal_enterococci"
                },
                "ecoli": {
                    "$first": "$ecoli"
                },
                "measurement_date": {
                    "$first": "$measurement_date"
                }
            }
        }
    ]))

    return {
        'statusCode': 200,
        'body': json.dumps(items, default=json_util.default)
    }