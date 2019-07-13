import json
import shutil
import urllib
from datetime import datetime

import requests
import boto3
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import re
from tika import parser

VARNA_FILE_NAME = "mvodi-2019.pdf"
VARNA_URL = "http://www.rzi-varna.com/health/mvodi-2019.pdf"


def degreesToDecimal(lat, lon):

    regex = r"\d{2}"

    latArray = re.findall(regex, lat.replace(' ',''))
    latitude = int(latArray[0]) + (int(latArray[1])/60) + (int(latArray[2])/3600)

    lonArray = re.findall(regex, lon.replace(' ',''))
    longitude = int(lonArray[0]) + (int(lonArray[1])/60) + (int(lonArray[2])/3600)

    return round(latitude, 6), round(longitude, 6)

def download(url, file_name):
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        r.raw.decode_content = True
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

def processFile(file_name):
    raw = parser.from_file(file_name)
    lineItems = raw['content'].split('\n')
    results = []
    tableResults = []

    for i in range(0, len(lineItems) - 1):
        if lineItems[i] is '':
            continue
        elif re.match("^\d{1,2}.\d{2}.\d{4} г.", lineItems[i]):
            measurement = lineItems[i].split(' ')
            parsedDate = None
            if type(measurement[2]) is datetime:
                parsedDate = measurement[2]
            else:
                parsedDate = datetime.strptime(measurement[2], "%d.%m.%Y")
            result = {
                'measurement_date': parsedDate.isoformat(),
                'intestinal_enterococci': measurement[5] if measurement[4] == 'под' else measurement[4],
                'ecoli': measurement[len(measurement) - 1]
            }
            tableResults.append(result)
        elif lineItems[i].startswith('Географски координати:'):
            coordinates = lineItems[i].split(' ')
            coord_x = coordinates[len(coordinates) - 1].strip('.')
            i += 2
            coordinates = lineItems[i].split(' ')
            coord_x += ' ' + coordinates[0]
            coord_y = coordinates[len(coordinates) - 1].strip('.')
            i += 3
            coord_y += ' ' + lineItems[i]
        elif lineItems[i].startswith('Пункт за вземане на проби'):
            pointIndex = lineItems[i].index('№')
            point = lineItems[i][pointIndex + 1: lineItems[i].index(' ', pointIndex + 2)].strip(' ').lstrip('.')
            nameIndex = lineItems[i].index('Наименование')
            name = lineItems[i][nameIndex + 12 : len(lineItems[i])].strip()
        elif lineItems[i].startswith('ДРУГИ ДОПЪЛНИТЕЛНО ИЗСЛЕДВАНИ ПРИ НЕОБХОДИМОСТ ПОКАЗАТЕЛИ'):
            for tr in tableResults:
                coordinates = degreesToDecimal(coord_x, coord_y);
                tr['coord_x'] = coordinates[0]
                tr['coord_y'] = coordinates[1]
                tr['point'] = point
                tr['name'] = name
                results.append(tr)

            tableResults = []
            point = ''
            name = ''

    return results

def lambda_handler(event, context):
    file_path = '/tmp/' + VARNA_FILE_NAME
    download(VARNA_URL, file_path)
    beaches_varna = processFile(file_path)

    # y = json.dumps(beaches_varna)
    # print(y)

    # INSERTTT----------------------------

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

    print(host)

    mongo_client = MongoClient('mongodb://%s:%s@%s:27017' % (username, password, host))

    db = mongo_client.seals
    try:
        db.beaches.insert_many(beaches_varna, ordered=False)
    except BulkWriteError as bwe:
        print(bwe.details)

    return {
        'statusCode': 204
    }