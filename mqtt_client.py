#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import time
import random
import json
import uuid

# MQTT broker configuration
# make sure to change it before using
MQTT_BROKER_HOST = "52.81.36.9"
MQTT_BROKER_PORT = 1883
MQTT_BROKER_KEEPALIVE = 60
MQTT_TOPIC = "sensor/temperature"
# username = 'rak'
# password = '123456'

def get_reportID():
    new_reportID = str(uuid.uuid1())
    return new_reportID


def get_reportTime():
    new_reportTime = round(time.time() * 1000)
    return new_reportTime


def get_DATA_CHANGED_message(sensorID):
    DATA_CHANGED_message = {"reportId": "",
                            "reportTime": "",
                            "data": []}
    DATA_CHANGED_message["reportId"] = get_reportID()
    DATA_CHANGED_message["reportTime"] = get_reportTime()
    # generate a random sensor reading between 5 and 50 and then round it
    sensor_DATA = []
    for i in range(0, sensorID+1):
        sensor_readings = round(random.uniform(5, 50))
        data_dict = {"sensorID": i, "Value": sensor_readings}
        DATA_CHANGED_message["data"].append(data_dict)
    return DATA_CHANGED_message

    # sensor_readings = round(random.uniform(5, 50), 2)
    # data_dict = {"sensorID": sensorID, "Value": sensor_readings}
    # DATA_CHANGED_message["data"].append(data_dict)
    # return DATA_CHANGED_message


# create a MQTT client
client = mqtt.Client()
# client.username_pw_set(username, password)

# connect to the MQTT broker
client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_BROKER_KEEPALIVE)

# start the MQTT loop in a separate thread
client.loop_start()

# send uplinks at a regular interval
while True:
    data = json.dumps(get_DATA_CHANGED_message(10)).encode('utf-8')
    # publish the sensor value to the MQTT broker
    client.publish(MQTT_TOPIC, data)

    # wait for 30 seconds before sending the next uplink
    time.sleep(30)
