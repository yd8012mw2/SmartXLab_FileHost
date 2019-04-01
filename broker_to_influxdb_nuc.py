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

consumer = KafkaConsumer('resource', 'data', bootstrap_servers=['192.168.1.4'])
partitions = consumer.poll(timeout)
consumer = KafkaConsumer('resource', 'data', bootstrap_servers=['192.168.1.4:9091'])

while partitions == None or len(partitions) == 0:
    message = next(consumer)

    print(message.topic)
    print(message.value)

    if message.topic == 'resource':
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
            print(cmd)

            subprocess.call([cmd], shell=True, stdout=FNULL)
        except Exception as e:
            print("RESOURCE EXCEPTION")
            print(e)

    elif message.topic == 'data':
        try:
            cmd = "curl -i -XPOST 'http://localhost:8086/write?db=Labs' --data-binary 'labs,hosts=Labs,region=GIST random=" + str(message.value).strip() + ",sent=False'"
            print(cmd)
            subprocess.call([cmd], shell=True, stdout=FNULL)

        except Exception as e:
            print("DATA EXCEPTION")
            print(e)


