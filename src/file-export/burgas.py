import json
import shutil
import urllib
from datetime import datetime

import boto3
import math
import requests
import pandas as pd
import re
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

BURGAS_FILE_NAME = "tablici_monitoring_moreMZ-2019.xls"
BURGAS_URL = "http://www.rzi-burgas.com/Vodi/tablici_monitoring_moreMZ-2019.xls"


def degreesToDecimal(lat, lon):

    regex = r"\d{2}"

    latArray = re.findall(regex, lat.replace(' ',''))
    latitude = int(latArray[0]) + (int(latArray[1])/60) + (int(latArray[2])/3600)

    lonArray = re.findall(regex, lon.replace(' ',''))
    longitude = int(lonArray[0]) + (int(lonArray[1])/60) + (int(lonArray[2])/3600)

    return round(latitude, 6), round(longitude, 6)


def is_nan(x):
    return (math.isnan(x) or x != x)


def download(url, file_name):
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        r.raw.decode_content = True
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(r.raw, f)


def extractXlsFile(file_name):
    df = pd.read_excel(file_name, header=None)
    return df.values

def procesMeasIndex(value):
    if isinstance(value, int):
        return value
    elif value.isnumeric():
        return int(value)
    elif value.startswith("под"):
        return 10
    else:
        return 0

def processMeasurement(row):
    if is_nan(row[2]) & is_nan(row[3]):
        return None

    date = row[1]

    if date != '':
        penter = row[2]
        esher = row[3]
        regex = r"\s*г."

        parsedDate = None
        if type(date) is datetime:
            parsedDate = date
        else:
            parsedDate = datetime.strptime(re.sub(regex,"", date), "%d.%m.%Y")
        parsedPenter = type

        parsedPenter = procesMeasIndex(penter)
        parsedEsher = procesMeasIndex(esher)

        measurement = {'date': parsedDate.isoformat(),
                       'penter': parsedPenter,
                       'ecoli': parsedEsher
                       }

        return measurement
    return None


def processBeach(dataArray, rowIndex):
    id = dataArray[rowIndex][2]
    title = dataArray[rowIndex][5]
    coordXStr = dataArray[rowIndex + 1][3]
    coordYStr = dataArray[rowIndex + 1][5]

    coordinates = degreesToDecimal(coordXStr, coordYStr);

    coordX = coordinates[0]
    coordY = coordinates[1]

    i = rowIndex
    print(len(dataArray))
    while i < len(dataArray) and dataArray[i][0] != 'Планирана дата за пробонабиране, съгласно графика за мониторинг' : i += 1

    mArr = []
    i+=1
    m = processMeasurement(dataArray[i])
    while m is not None:
        mArr.append(m)
        i+=1
        m = processMeasurement(dataArray[i])


    result = []
    for measurement in mArr:
        result.append({
            'point': id,
            'name': title,
            'coord_x': coordX,
            'coord_y': coordY,
            'measurement_date': measurement['date'],
            'intestinal_enterococci': measurement['penter'],
            'ecoli': measurement['ecoli']
        })
    return result

def processBurgas(dataArray):
    i = 0
    beaches = []
    while i<len(dataArray):
        if dataArray[i][0] == 'ПУНКТ ЗА ВЗЕМАНЕ НА ПРОБИ №':
            b = processBeach(dataArray,i)
            beaches += b
        i += 1
    return beaches


def lambda_handler(event, context):
    file_path = '/tmp/' + BURGAS_FILE_NAME
    download(BURGAS_URL, file_path)

    rawDataBurgas = extractXlsFile(file_path)
    burgasBeaches = processBurgas(rawDataBurgas)

    # INSERTTT----------------------------

    secret_name = "MONGO_URI"
    region_name = "eu-central-1"

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
        db.beaches.insert_many(burgasBeaches, ordered=False)
    except BulkWriteError as bwe:
        print(bwe.details)

    return {
        'statusCode': 204
    }