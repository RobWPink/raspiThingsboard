import paho.mqtt.client as mqtt
#import sqlite3 as lite
import sys, time, serial, struct,re,os,subprocess
from datetime import datetime
host = '34.236.51.120'
port = 1883
telemetry = 'v1/devices/me/telemetry'
attributes = 'v1/devices/me/attributes'
password = "test1234"
try:
  username = sys.argv[1]
except:
  print("Missing username argument!")
  sys.exit(1)

prev_cmd = ""
flushData = False
passed = False
allCnt = 0
sendAll = False
allData = {
    "time": 0.0,
    "tt511": 0.0,
    "tt512": 0.0,
    "burnerTemp": 0.0,
    "tt513": 0.0,
    "tt514": 0.0,
    "tt407": 0.0,
    "tt408": 0.0,
    "tt410": 0.0,
    "tt411": 0.0,
    "tt430": 0.0,
    "tt441": 0.0,
    "tt442": 0.0,
    "tt443": 0.0,
    "tt444": 0.0,
    "tt445": 0.0,
    "tt446": 0.0,
    "tt447": 0.0,
    "tt448": 0.0,
    "tt449": 0.0,
    "tubeMean": 0.0,
    "tubeMax": 0.0,
    "tt142": 0.0,
    "tt301": 0.0,
    "tt303": 0.0,
    "tt306": 0.0,
    "tt313": 0.0,
    "tt319": 0.0,
    "bmmAlarm": 0,
    "bmmProof": 0,
    "estop": 0,
    "greenButton": 0,
    "greenPilot": 0,
    "amberButton": 0,
    "amberPilot": 0,
    "psh": 0,
    "psl": 0,
    "zsl": 0,
    "bmmRun": 0,
    "xv501": 0,
    "xv217": 0,
    "xv474": 0,
    "xv1100": 0,
    "xv122": 0,
    "twv308": 0,
    "twv310": 0,
    "twv308FeedBackOpen": 0,
    "twv308FeedBackClosed": 0,
    "twv310FeedBackOpen": 0,
    "twv310FeedBackClosed": 0,
    "fcv134FeedBack": 0.0,
    "bl508FeedBack": 0.0,
    "pmp204FeedBack": 0.0,
    "fcv549FeedBack": 0.0,
    "pt654": 0.0,
    "pt304": 0.0,
    "pt420": 0.0,
    "pt383": 0.0,
    "pt318": 0.0,
    "ft132": 0.0,
    "ft219": 0.0,
    "bl508": 0.0,
    "fcv134": 0.0,
    "pmp204": 0.0,
    "fcv141": 0.0,
    "fcv205": 0.0,
    "fcv549": 0.0,
    "fcv141FeedBack": 0.0,
    "fcv205FeedBack": 0.0,
    "pt100": 0.0,
    "psaON":0,
    "psaReady":0,
    "psaACK":0,
    "psaFail":0,
    "autotwv":0,
  }
allowed_commands = ("bl508","bmmRun","fcv134","fcv141","fcv205","fcv549","legacy","pmp204","twv308","twv310","xv501","xv217","xv474","xv1100","xv122","psaReady","psaACK","psaON")

try:
  ser = serial.Serial(
    port='/dev/ttyACM0',
    baudrate = 9600,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
  )
  passed = True
except serial.serialutil.SerialException or  FileNotFoundError:
  passed = False
  
def on_message_attributes(client, userdata, msg):
  global prev_cmd
  global flushData
  global sendAll
  global ser
  print('Received: ', str(msg.payload.decode('utf-8')))
  try:
    received = msg.payload.decode()
    output = ""
    if 'Invalid entry' in received:
      received = prev_cmd
      print("serialSend error, retrying")
    if 'ctl' in received:
      flushData = True
      received = received.replace("ctl","")
      number = re.search('{".*":(.+?)}', received).group(1)
      name = re.search('{"(.+?)":.*}', received).group(1)
      if name in allowed_commands:
        if name == "bmmRun":
          name = "bmm"
        
        if name == "psaON":
          if 'false' in number:
            name = "PSA_OFF"
          elif 'true' in number:
            name = "PSA_ON"
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
        prev_cmd = output
        ser.write(bytes(output,'utf-8'))
  except Exception as e:
    print(e)

client = mqtt.Client(username)
client.username_pw_set(username, password)
client.connect(host, port)
#client.on_publish = on_publish
client.message_callback_add(attributes, on_message_attributes)
client.connect('34.236.51.120', 1883)
client.loop_start()
client.subscribe(attributes)

def main():
  global passed
  global allData
  global client
  global allCnt
  global flushData
  global ser
  try:
    while True:
      while not passed:
        try:
          ser = serial.Serial(
            port='/dev/ttyACM0',
            baudrate = 9600,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            bytesize=serial.EIGHTBITS,
          )
          if "200 OK" in os.popen('curl -Is  http://www.google.com | head -n 1').read():
            passed = True
            os.execl(sys.executable, sys.executable, *sys.argv)
        except serial.serialutil.SerialException or FileNotFoundError:
          passed = False
      
      while passed:
        if not "200 OK" in os.popen('curl -Is  http://www.google.com | head -n 1').read():
          passed = False
          print("connection error")
          continue
        try:
          raw=ser.readline()
          print(raw)
          if len(raw) < 2:
            print(0)
            
          elif 'OK'.encode() in raw:
            ser.flush()
            time.sleep(1)
            
          elif ','.encode() in raw:
            parsed = raw.split(','.encode())
            parsed[-1].replace(bytes('\r\n','utf-8'),bytes('','utf-8'))
            try:
              data = [float(i) for i in parsed]
              if len(data) == len(allData):
                time.sleep(0.5)
                i = 0
                j = 0
                msg = "{"
                if allCnt > 10:
                  allCnt = 0
                  sendAll = True
                else:
                  allCnt = allCnt + 1
                  sendAll = False
                changed = [" "] * len(allData)
                for key in allData:
                  if flushData:
                    ser.flushInput()
                    ser.flushOutput()
                    time.sleep(0.1)
                    flushData = False
                    break
                  if not allData[key] == data[i] or sendAll:
                    allData[key] = data[i]
                    changed[j] = key
                    j = j + 1
                  i = i + 1
                i = 0
                j = 0
                for changedKey in changed:
                  if not changedKey == " ": 
                    msg = msg + "\"" + changedKey + "\":" + str(allData[changedKey]) + ","
                    if i % 2 == 0 or changed[i+1] == " ":
                      msg = msg[:-1]
                      msg = msg + '}'
                      print(msg)
                      result = client.publish(telemetry, msg)
                      status = result[0]
                      if not status == 0:
                        print(f"Failed to send message to topic {telemetry}")
                        msg = "{"
                      else:
                        time.sleep(0.1)
                        msg = "{"
                    i = i + 1
                  else:
                    changed = [" "] * len(allData)
                    break
                  
                ser.reset_input_buffer()
                
            except Exception as e:
              print(e)
        except serial.serialutil.SerialException or FileNotFoundError:
          passed = False
  except KeyboardInterrupt:
    print('Interrupted')
    
if __name__ == '__main__':
  main()

