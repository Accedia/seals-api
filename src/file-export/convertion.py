import math
import re

def degreesToDecimal(lat, lon):
    regex = r"\d{2}"

    latArray = re.findall(regex, lat)
    latitude = int(latArray[0]) + (int(latArray[1])/60) + (int(latArray[2])/3600)

    lonArray = re.findall(regex, lon)
    longitude = int(lonArray[0]) + (int(lonArray[1])/60) + (int(lonArray[2])/3600)

    return round(latitude, 6), round(longitude, 6)


coordX = 'N 42° 49\' 14"'
coordY = 'E 27° 53\' 07"'

print(degreesToDecimal(coordX, coordY)[0])
