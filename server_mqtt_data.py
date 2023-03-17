#!/usr/bin/env python3

"""
Modbus/TCP server with virtual data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Map the system date and time to @ 0 to 5 on the "holding registers" space.
Only the reading of these registers in this address space is authorized. All
other requests return an illegal data address except.

Run this as root to listen on TCP priviliged ports (<= 1024).
"""

import argparse
from pyModbusTCP.server import ModbusServer, DataBank
from datetime import datetime
from paho.mqtt import client as mqtt_client
import json

MQTT_BROKER_HOST = '52.81.36.9'
MQTT_BROKER_PORT = 1883
MQTT_TOPIC = "sensor/temperature"

last_payload = None


class MyDataBank(DataBank):
    """A custom ModbusServerDataBank for override get_holding_registers method."""

    def __init__(self):
        # turn off allocation of memory for standard modbus object types
        # only "holding registers" space will be replaced by dynamic build values.
        super().__init__(virtual_mode=True)

    def get_holding_registers(self, address, number=1, srv_info=None):
        """Get virtual holding registers."""
        # populate virtual registers dict with current datetime values
        # now = datetime.now()
        # v_regs_d = {0: now.day, 1: now.month, 2: now.year,
        #             3: now.hour, 4: now.minute, 5: now.second}
        v_regs_d = {}
        try:
            for i in range(0, len(last_payload["data"])):
                key = last_payload["data"][i]["sensorID"]
                value = last_payload["data"][i]["Value"]
                v_regs_d.update({key: value})
        except Exception as e:
            print(e)
        print(v_regs_d)
        # build a list of virtual regs to return to server data handler
        # return None if any of virtual registers is missing
        try:
            return [v_regs_d[a] for a in range(address, address + number)]
        except KeyError:
            return


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        global last_payload
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        last_payload = json.loads(msg.payload.decode())

    client.subscribe(MQTT_TOPIC)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_start()


if __name__ == '__main__':
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', type=str, default='10.2.13.27', help='Host (default: localhost)')
    parser.add_argument('-p', '--port', type=int, default=502, help='TCP port (default: 502)')
    args = parser.parse_args()
    # init modbus server and start it
    server = ModbusServer(host=args.host, port=args.port, data_bank=MyDataBank())
    run()
    server.start()
