from influxdb import InfluxDBClient
from time import sleep
import os

ip = '192.168.1.4'

client = InfluxDBClient(host='localhost', port=8086, database='labs')

latest_time = '2016-07-31T20:07:00Z'
initialized = False
while True:
  result = client.query('select "random" from "Labs"."autogen"."labs" order by "time" DESC LIMIT 1')
  cnt = 0

  for col in result.raw['series'][0]['columns']:
    if col == 'time':
      break
    cnt = cnt + 1

  prev_latest_time = latest_time
  latest_time = result.raw['series'][0]['values'][0][cnt]

  if not initialized:
    initialized = True
    continue

  result = client.query('select "random" from "Labs"."autogen"."labs" where "time" <= ' + "'" + latest_time + "'" + ' AND "time" > ' + "'" + prev_latest_time + "'")

  try:
    result.raw['series'][0]['values'] # Error Checker
    cnt = 0

    for col in result.raw['series'][0]['columns']:
      if col == 'random':
        break
      cnt = cnt + 1
    
    for data in result.raw['series'][0]['values']:
      cmd = str(int(data[cnt]) / 10) + "," + ip
      os.system("echo " + cmd)

  except:
    pass

  sleep(1)

