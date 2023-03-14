from paho.mqtt import client as mqtt_client
import sqlite3 as lite
from threading import Thread
import sys, time, serial, struct,re
from datetime import datetime

host = '3.142.96.59'
port = 1883
telemetry = 'v1/devices/me/telemetry'
attributes = 'v1/devices/me/attributes'

try:
  clientId = sys.argv[1]
except:
  clientId = 'unknownID'
  
try:
  username = sys.argv[2]
except:
  username = 'v1Tu9MBLtUhNbs2ygGKf'

try:
  password = sys.argv[3]
except:
  password = 'test1234'

#[time,tt511,tt512,burnerTemp,tt513,tt514,tt407,tt408,tt410,tt411,tt430,tt441,tt442,tt443,tt444,tt445,tt446,tt447,tt448,tt449,tubeMean,tubeMax,tt142,tt301,tt303,tt306,tt313,tt319,bmmAlarm,bmmProof,estop,greenButton,greenPilot,amberButton,amberPilot,psh,psl,zsl,bmmRun,xv501,xv217,xv474,xv1100,xv122,twv308,twv310,fcv134FeedBack,bl508FeedBack,pmp204FeedBack,fcv549FeedBack,pt654,pt304,pt420,pt383,pt318,ft132,ft219,bl508,fcv134,pmp204,fcv141,fcv205,fcv549]
prev_msg = ''
sendAll = True
flushData = False
allData = {
  "time" : 0,
  "tt511" : 0,
  "tt512" : 0,
  "burnerTemp" : 0,
  "tt513" : 0,
  "tt514" : 0,
  "tt407" : 0,
  "tt408" : 0,
  "tt410" : 0,
  "tt411" : 0,
  "tt430" : 0,
  "tt441" : 0,
  "tt442" : 0,
  "tt443" : 0,
  "tt444" : 0,
  "tt445" : 0,
  "tt446" : 0,
  "tt447" : 0,
  "tt448" : 0,
  "tt449" : 0,
  "tubeMean" : 0,
  "tubeMax" : 0,
  "tt142" : 0,
  "tt301" : 0,
  "tt303" : 0,
  "tt306" : 0,
  "tt313" : 0,
  "tt319" : 0,
  "bmmAlarm" : 0,
  "bmmProof" : 0,
  "estop" : 0,
  "greenButton" : 0,
  "greenPilot" : 0,
  "amberButton" : 0,
  "amberPilot" : 0,
  "psh" : 0,
  "psl" : 0,
  "zsl" : 0,
  "bmmRun" : 0,
  "xv501" : 0,
  "xv217" : 0,
  "xv474" : 0,
  "xv1100" : 0,
  "xv122" : 0,
  "twv308" : 0,
  "twv310" : 0,
  "fcv134FeedBack" : 0,
  "bl508FeedBack" : 0,
  "pmp204FeedBack" : 0,
  "fcv549FeedBack" : 0,
  "pt654" : 0,
  "pt304" : 0,
  "pt420" : 0,
  "pt383" : 0,
  "pt318" : 0,
  "ft132" : 0,
  "ft219" : 0,
  "bl508" : 0,
  "fcv134" : 0,
  "pmp204" : 0,
  "fcv141" : 0,
  "fcv205" : 0,
  "fcv549" : 0
}
try:
  ser = serial.Serial(
    port='/dev/ttyACM0',
    baudrate = 9600,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
    #timeout=none
  )
  passed = True
except serial.serialutil.SerialException:
  passed = False
  ser.close()

class sendDataProgram:
  def __init__(self):
    self._running = True
  
  def terminate(self):  
    self._running = False  
  
  def run(self):
    def publish(client,topic):
      global allData
      global flushData
      global all
      while True:
        time.sleep(0.5)
        if passed:
          raw=ser.readline()
          print(raw)
          if len(raw) < 2:
            print(0)
          elif 'OK'.encode() in raw:
            ser.flush()
            time.sleep(1)
            print('inputting "sql"')
            ser.write(bytes('sql','utf-8'))
          elif ','.encode() in raw:
            parsed = raw.split(','.encode())
            parsed[-1].replace(bytes('\r\n','utf-8'),bytes('','utf-8'))
            try:
              data = [float(i) for i in parsed]
              if len(data) == 63:
                i = 0 
                msg = '['
                for key in allData:
                  if flushData:
                    ser.flushInput()
                    ser.flushOutput()
                    time.sleep(0.1)
                    flushData = False
                    break
                  if not allData[key] == data[i] or sendAll:
                    msg = msg + '{"'+key+'":'+str(allData[key])+'},' 
                    allData[key] = data[i]
                    if i % 10 == 0 or i == 62:
                      msg = msg[:-1]
                      msg = msg + ']'
                      print(msg)
                      result = client.publish(topic, msg)
                      status = result[0]
                      if not status == 0:
                        print(f"Failed to send message to topic {topic}")
                        msg = '['
                      else:
                        msg = '['
                        time.sleep(0.25)
                    i = i + 1 #for loop int increment
                  ser.reset_input_buffer()
                
            except Exception as e:
              print(e)
          else:
            pass
      
    global client

    client.loop_start()
    publish(client,telemetry)

class receiveDataProgram:
  def __init__(self):
    self._running = True
  
  def terminate(self):  
    self._running = False  
  
  def run(self):
    
    def subscribe(client: mqtt_client):
      def on_message(client, userdata, msg):
        global sendAll
        global flushData
        if passed:
          try:
            received = msg.payload.decode()
            print(received)
            output = ""
            if 'ctl' in received:
              flushData = True
              received = received.replace("ctl","")
              number = re.search('{".*":(.+?)}', received).group(1)
              name = re.search('{"(.+?)":.*}', received).group(1)
              if name == "bmmRun":
                name = "bmm"
              if 'false' in number or 'true' in number:
                output = name
              else:
                output = name +" "+ number
            elif "legacy" in received and not "deleted" in received:
              if "all" in received:
                sendAll = not sendAll
                output = ''
              else:
                output = re.search('{"legacy":"(.+?)"}', received).group(1)
            elif "deleted" in received:
              output = ''

            if not output == '':
              print(output)
              ser.write(bytes(output,'utf-8'))
              #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
          except Exception as e:
            print(e)
      client.subscribe(attributes)
      client.on_message = on_message
    subscribe(client)
    #client.loop_forever() #this is commented since two loops cannot happen even in different threads, using the other loop rn

def connect_mqtt():
  global client
  def on_connect(client, userdata, flags, rc):
    if rc == 0:
      print("Connected to MQTT Host!")
    else:
      print("Failed to connect, return code %d\n", rc)

  client = mqtt_client.Client(clientId)
  client.username_pw_set(username, password)
  client.on_connect = on_connect
  client.connect(host, port)
  return client

def main():
  
  try:
    global client
    global passed
    client = connect_mqtt()
    
    sendData = sendDataProgram()
    sendDataThread = Thread(target=sendData.run) 
    sendDataThread.start()

    receiveData = receiveDataProgram()
    receiveDataThread = Thread(target=receiveData.run) 
    receiveDataThread.start()
    while(1):
      while not passed:
        try:
          ser = serial.Serial(
            port='/dev/ttyUSB0',
            baudrate = 9600,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            bytesize=serial.EIGHTBITS,
            #timeout=none
          )
          passed = True
        except serial.serialutil.SerialException:
          passed = False
      while passed:
        try:
          pass
        except serial.serialutil.SerialException:
          passed = false
          ser.close
        
      
  except KeyboardInterrupt:
    print('Interrupted')
    sendData.terminate()
    receiveData.terminate()
    print("Goodbye")
if __name__ == '__main__':
    main()


