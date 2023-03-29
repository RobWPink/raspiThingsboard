from paho.mqtt import client as mqtt_client
import sqlite3 as lite
from threading import Thread
import sys, time, serial, struct,re
from datetime import datetime

host = '34.236.51.120'
port = 1883
telemetry = 'v1/devices/me/telemetry'
attributes = 'v1/devices/me/attributes'


password = 'test1234'



def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client('reformer1_pad')
    client.username_pw_set('reformer1_pad', 'test1234')
    client.on_connect = on_connect
    client.connect('34.236.51.120', 1883)
    return client


def publish(client):
    msg_count = 0
    while True:
        time.sleep(1)
        msg = "{\"value1\":20,\"value2\":40}"
        result = client.publish('v1/devices/me/telemetry', msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic 'v1/devices/me/telemetry'")
        else:
            print(f"Failed to send message to topic 'v1/devices/me/telemetry'")
        msg_count += 1


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()