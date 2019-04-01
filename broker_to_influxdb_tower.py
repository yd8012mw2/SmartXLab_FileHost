import threading, logging, time
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

timeout = 100
actual_data=[]

FNULL = open(os.devnull, 'w')

consumer = KafkaConsumer('summary', bootstrap_servers=['192.168.1.2'])
partitions = consumer.poll(timeout)
consumer = KafkaConsumer('summary', bootstrap_servers=['192.168.1.2:9091'])

while partitions == None or len(partitions) == 0:
  message = next(consumer)

  print(message.value)
  try:
    random = str(message.value).split(',')[0]
    ip = str(message.value).split(',')[1]
    cmd = "curl -i -XPOST 'http://localhost:8086/write?db=Labs' --data-binary 'labs,hosts=Labs,region=GIST random=" + random + ",ip=\"" + ip + "\"'"
    print(cmd)
    subprocess.call([cmd], shell=True, stdout=FNULL)
  except Exception as e:
    print("EXCEPTION")
    print(e)

