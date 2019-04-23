import threading, logging, time, re
import multiprocessing
import msgpack

from kafka import TopicPartition
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import requests, json
import time
import os
import sys
import subprocess
import urllib, urllib2

from time import localtime, strftime

cmd ="curl -XPOST 'http://localhost:8086/query' --data-urlencode 'q=CREATE DATABASE 'Labs''"
subprocess.call([cmd], shell=True)

cmd ="curl -XPOST 'http://localhost:8086/query' --data-urlencode 'q=CREATE DATABASE 'ssh''"
subprocess.call([cmd], shell=True)

timeout = 100
actual_data=[]

FNULL = open(os.devnull, 'w')

consumer = KafkaConsumer('resource', 'sshlog', bootstrap_servers=['192.168.1.2'])
partitions = consumer.poll(timeout)
consumer = KafkaConsumer('resource', 'sshlog', bootstrap_servers=['192.168.1.2:9091'])

while partitions == None or len(partitions) == 0:
  message = next(consumer)

  if message.topic == 'resource':
    print("Resource is in")

    try:
      j = json.loads(message.value)

      cmd = "curl -i -XPOST 'http://localhost:8086/write?db=Labs' --data-binary 'labs,hosts=Labs,region=GIST "
      for i in j.keys():
        try:
          float(j[i])
          cmd = cmd + i + "=" + str(j[i]) + ','
        except:
          cmd = cmd + i + "=" + '"' + str(j[i]) + '",'

      cmd = cmd[:-1]
      cmd = cmd + "'"
      #print(cmd)

      subprocess.call([cmd], shell=True, stdout=FNULL)
    except Exception as e:
      print("RESOURCE EXCEPTION")
      print(e)


  elif message.topic == 'sshlog':
    if 'Failed password' in message.value:
      print('SSH IN')
      ip = re.findall( r'[0-9]+(?:\.[0-9]+){3}', message.value)
      cmd = "curl -i -XPOST 'http://localhost:8086/write?db=ssh' --data-binary 'ssh,hosts=ssh,region=GIST "
      cmd = cmd + "tried=" + '"' + ip[0] + '"' + "," + "success=false'"

      print(cmd)
      subprocess.call([cmd], shell=True, stdout=FNULL)
      print('SSH FAILURE from ' + ip[0])

    elif 'Accepted password' in message.value:
      ip = re.findall( r'[0-9]+(?:\.[0-9]+){3}', message.value)
      cmd = "curl -i -XPOST 'http://localhost:8086/write?db=ssh' --data-binary 'ssh,hosts=ssh,region=GIST "
      cmd = cmd + "tried=" + '"' + ip[0] + '"' + "," + "success=true'"

      print('SSH IN')
      print(cmd)
      subprocess.call([cmd], shell=True, stdout=FNULL)
      print('SSH SUCCESS from ' + ip[0])

  """try:
    random = str(message.value).split(',')[0]
    ip = str(message.value).split(',')[1]
    cmd = "curl -i -XPOST 'http://localhost:8086/write?db=Labs' --data-binary 'labs,hosts=Labs,region=GIST random=" + random + ",ip=\"" + ip + "\"'"
    print(cmd)
    subprocess.call([cmd], shell=True, stdout=FNULL)
  except Exception as e:
    print("EXCEPTION")
    print(e)"""

