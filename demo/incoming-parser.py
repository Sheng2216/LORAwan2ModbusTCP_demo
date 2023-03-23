#!/usr/bin/env python3

"""
introduction here
"""

from paho.mqtt import client as mqtt_client
import json
import time
import sqlite3
import toml

data = toml.load("config.yml")

# Load MQTT_broker related configuration
MQTT = data['MQTT_broker']
MQTT_BROKER_HOST = MQTT['host']
MQTT_BROKER_PORT = MQTT['port']
MQTT_BROKER_USERNAME = MQTT['username']
MQTT_BROKER_PASSWORD = MQTT["password"]

# load MQTT topic
TOPIC = data['MQTT_topic']
SENSOR_UPLINK = TOPIC["uplink"]

# Load database related configuration
General = data['DATABASE']
DATABASE_PATH = General['database_path']


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client()
    if MQTT_BROKER_USERNAME and MQTT_BROKER_PASSWORD:
        client.username_pw_set(MQTT_BROKER_USERNAME, MQTT_BROKER_PASSWORD)
    client.on_connect = on_connect
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        last_payload = json.loads(msg.payload.decode())
        devEUI = last_payload["deviceInfo"]["devEui"]
        raw_data = last_payload["data"]
        # Query devEUI from table for mapping them to unit_id
        cur.execute("SELECT devEUI FROM sensor_data_table")
        original_devEUI_rows = cur.fetchall()
        original_devEUI_rows.sort()
        devEUI_rows = []
        # A new list to check whether the devEUI existed or not
        for item in original_devEUI_rows:
            devEUI_rows.append(item[0])

        # Append the new devEUI to the original list if it doesn't exist before
        if devEUI not in devEUI_rows:
            devEUI_rows.append(devEUI)
        # Map devEUI to a integer in a dictionary
        devEUI_dict = {k: v for v, k in enumerate(sorted(set(devEUI_rows)))}
        # print(devEUI_dict)
        unit_id = devEUI_dict[devEUI]
        timestamp = int(time.time())

        # Check if a record with the specified unit_id already exists
        cur.execute("SELECT COUNT(*) FROM sensor_data_table WHERE unit_id=?", (unit_id,))
        result = cur.fetchone()[0]
        if result == 0:
            # If no record exists, insert a new one
            cur.execute("INSERT INTO sensor_data_table (unit_id, devEUI, raw_data, timestamp) VALUES (?, ?, ?, ?)",
                        (unit_id, devEUI, raw_data, timestamp))
        else:
            # If a record already exists, update the devEUI, raw_data, and timestamp fields
            cur.execute("UPDATE sensor_data_table SET devEUI=?, raw_data=?, timestamp=? WHERE unit_id=?",
                        (devEUI, raw_data, timestamp, unit_id))

        data = [(devEUI, raw_data, unit_id, timestamp)]
        print(data)
        con.commit()  # Remember to commit the transaction after executing INSERT

    client.subscribe(SENSOR_UPLINK)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':

    # Create a database called lorawan_sensor_data
    con = sqlite3.connect(DATABASE_PATH)
    cur = con.cursor()

    # Check whether the table exists or not
    cur.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='sensor_data_table' ''')
    if cur.fetchone()[0] == 1:
        print('sensor_data_table exists, continue...')
    else:
        # Create a table called sensor_data
        print('table sensor_data_table not found, creating a new one under /databases')
        cur.execute(
            "CREATE TABLE sensor_data_table(devEUI, raw_data, unit_id, timestamp)")

    run()  # Connects to MQTT
    con.close()  # Verify that the database has been written to disk
