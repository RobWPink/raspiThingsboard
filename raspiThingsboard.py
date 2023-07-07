import paho.mqtt.client as mqtt
import time, logging,sys,struct,re,os,subprocess,serial,optparse,threading
import logging.handlers as handlers
import os.path
import RPi.GPIO as GPIO

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
GPIO.setup(27, GPIO.OUT)
GPIO.output(27, GPIO.HIGH)

usage = "usage: %prog [options] arg1 arg2..."
parser = optparse.OptionParser(usage=usage)

parser.add_option('-u','--username',action="store",type="string",dest='username',help ='specify the MQTT username/clientID to connect to. (Required)')
parser.add_option('-p','--password',action="store",type="string",dest='password',help ='specify the MQTT password related to specified username.',default="test1234")
parser.add_option('-i','--ip',action="store",type="string",dest='host',help ='specify the MQTT password related to specified username.',default="34.236.51.120")
parser.add_option('-o','--port',action="store",type="string",dest ='serialDevice',help='Specify serial device tty port.',default="/dev/ttyACM0")
parser.add_option('-d','--debug',action="store_true",dest ='debug',help='Print out debug messages.')
parser.add_option('-q','--quiet',action="store_true",dest ='quiet',help='Silence all stdout messages.')


(options, args) = parser.parse_args()
if not options.username:   # if filename is not given
  parser.error('Username not given')
if options.debug and options.quiet:
  parser.error("options -q and -d are mutually exclusive")
mqttPort = 1883
telemetry = 'v1/devices/me/telemetry'
attributes = 'v1/devices/me/attributes'
allowed_commands = ("bl508","bmmRun","fcv134","fcv141","fcv205","fcv549","legacy","pmp204","twv308","twv310","xv501","xv217","xv474","xv1100","xv122","psaReady","psaACK","psaON")
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

log = logging.getLogger(options.username)
log.setLevel(logging.DEBUG if options.debug else logging.INFO)
if not options.quiet:
  handler = logging.StreamHandler(sys.stdout)
  handler.setLevel(logging.DEBUG if options.debug else logging.INFO)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  handler.setFormatter(formatter)
  log.addHandler(handler)

rotateHandler = handlers.RotatingFileHandler("/home/defaultUser/"+options.username+"Log.log", maxBytes=200000,backupCount=5)
rotateHandler.setLevel(logging.INFO)
rotateFormatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotateHandler.setFormatter(rotateFormatter)
log.addHandler(rotateHandler)

flushData = False
def rebootRestart(errorMessage,errorType):
  f = open('/home/defaultUser/raspiThingsboard/restartCnt', r);
  if not errorType in f.readline():
    log.critical(errorMessage+" Attempting reboot to resolve the issue.")
    f.close()
    f = open('/home/defaultUser/raspiThingsboard/restartCnt',w)
    f.write(errorType)
    f.close()
    client.loop_stop()
    os.system("echo 'oneh2' | sudo -S reboot")
    sys.exit(1)
  else:
    log.critical(errorMessage+" Reboot did not resolve the problem. Check connections and manually reboot.")
    f.close()
    f = open('/home/defaultUser/raspiThingsboard/restartCnt',w)
    f.write("")
    f.close()
    client.loop_stop()
    sys.exit(1)
    

def serialConnect():
  #check if we can connect to device
  serialTimeout = 0
  global ser
  global rebootCnt
  global restartCnt
  while True:
    try:
      ser = serial.Serial(
        port=options.serialDevice,
        baudrate = 9600,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        bytesize=serial.EIGHTBITS,
      )
      break
    except serial.serialutil.SerialException or FileNotFoundError:
      log.error("Unable to connect to serial device. Retrying...")
      time.sleep(1)
      serialTimeout = serialTimeout + 1
      if serialTimeout > 10:
        rebootRestart("Unable to connect to serial device.","serialDisconnect")


#########################################################################################################################################################
#########################################################################################################################################################
#########################################################################################################################################################
def main():
  serialConnect()
  global ser
  global flushData
  errCnt = 0
  try:
    allCnt = 0
    while True:
      try:
        raw=ser.readline()# grap raw serial data
        log.debug(raw)
        if len(raw) < 2: # Make sure its actually data
          pass
          
        elif 'OK'.encode() in raw:
          ser.flush()
          time.sleep(1)
          
        elif ','.encode() in raw: # Check if we are receiving listed data
          parsed = raw.split(','.encode()) # convert sting list into python list of strings
          parsed[-1].replace(bytes('\r\n','utf-8'),bytes('','utf-8')) # chop off extra special chars
          try:
            data = [float(i) for i in parsed] # Convert all stringed numbers into floats
            if len(data) == len(allData): # Only process if we have ALL of the data
              time.sleep(0.5)
              i = 0
              j = 0
              msg = "{"
              if allCnt > 10: # Every 10 messages send ALL data 
                allCnt = 0
              else:
                allCnt = allCnt + 1
              changed = [" "] * len(allData) # make list of all data that has changed since last loop
              for key in allData:
                if flushData:
                  ser.flushInput()
                  ser.flushOutput()
                  time.sleep(0.1)
                  flushData = False
                  break
                if not allData[key] == data[i] or allCnt == 10:
                  allData[key] = data[i]
                  changed[j] = key
                  j = j + 1
                i = i + 1
              i = 0
              j = 0
              for changedKey in changed:
                if not changedKey == " ": 
                  msg = msg + "\"" + changedKey + "\":" + str(allData[changedKey]) + ","
                  if i % 4 == 0 or changed[i+1] == " ":
                    msg = msg[:-1]
                    msg = msg + '}'
                    log.debug(msg)
                    result = client.publish(telemetry, msg)
                    status = result[0]
                    if not status == 0:
                      log.warning(f"Failed to send message to topic {telemetry}")
                      msg = "{"
                    else:
                      time.sleep(0.5)
                      msg = "{"
                  i = i + 1
                else:
                  changed = [" "] * len(allData)
                  break
                
              ser.reset_input_buffer()
              
          except Exception as e:
            if not "could not convert string to float" in str(e) and not "list index out of range" in str(e):
              log.warning("Parsing Failure: " + str(e))
              
        errCnt = 0
        
      except serial.serialutil.SerialException or FileNotFoundError:
        serialConnect()
      except Exception as e:
        if errCnt == 0:
          log.error(str(e))
          errCnt = errCnt + 1
        elif errCnt > 5:
          errCnt = 0
          rebootRestart("Parsing Error Occured: "+str(e),"parse")
        else:
          errCnt = errCnt + 1
        
  except KeyboardInterrupt:
    log.info('Manual User Shutdown Initiatated')
    client.loop_stop()
    sys.exit(0)
  except Exception as e:
    rebootRestart("Critical Error Occured: "+str(e),str(e))
    
    
    
    
    

#########################################################################################################################################################
#########################################################################################################################################################
#########################################################################################################################################################



#########################################################################################################################################################
# Daemon for checking internet and serial connection
def connectionCheck(serialPort, interval=1):
  #Check for internet connection
  internetCnt = 0
  while True:
    if "20" in os.popen('curl -Is  https://www.google.com | head -n 1').read():
      internetCnt = 0
    else:
      time.sleep(1)
      internetCnt = internetCnt + 1
    if internetCnt > 10:
      log.critical("Unable to connect to internet. Error Code:" + str(os.popen('curl -Is  https://www.google.com | head -n 1').read()) +". Exiting.")
      client.loop_stop()
      sys.exit(1)
    #continually check for existance of serial device
    # if not os.path.isfile(options.serialDevice):
      # log.critical("Serial Device at " + options.serialDevice + " has been disconnected. Exiting.")
      # client.loop_stop()
      # sys.exit(1)
      
    time.sleep(interval)

#########################################################################################################################################################
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
  log.info("Connected with result code "+str(rc))
  
  # Subscribing in on_connect() means that if we lose the connection and
  # reconnect then subscriptions will be renewed.
  client.subscribe(attributes,qos=1)

#########################################################################################################################################################
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
  global prev_cmd
  global flushData
  log.info("Received:"+str(msg.payload.decode()))
  try:
    received = msg.payload.decode()
    output = ""
    if 'Invalid entry' in received:
      received = prev_cmd
      log.warning("serialSend error, retrying")
    elif 'ctl' in received:
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
          
        elif "legacy" in received and not "deleted" in received:
          output = re.search('{"legacy":"(.+?)"}', received).group(1)
          
        else:
          output = name +" "+ number
      elif "deleted" in received:
        output = ''

      if not output == '':
        log.info("Fowarding command: " + output)
        prev_cmd = output
        ser.write(bytes(output,'utf-8'))
  except Exception as e:
    log.error(e)

#########################################################################################################################################################
# The callback for when we receive an PUBACK from server
def on_publish(client,userdata,mid):
  log.debug("Successfully published.")
#########################################################################################################################################################
# The callback for when we receive an SUBACK from server
def on_subscribe(client,userdata,mid,granted_qos):
  log.info("Subscription confirmed.")

#########################################################################################################################################################
# The callback for when we disconnect unintentionally 
def on_disconnect(client, userdata, rc):
  FIRST_RECONNECT_DELAY = 1
  RECONNECT_RATE = 2
  MAX_RECONNECT_COUNT = 12
  MAX_RECONNECT_DELAY = 60
  log.warning("Disconnected with result code: %s", rc)
  reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
  while reconnect_count < MAX_RECONNECT_COUNT:
    log.info("Reconnecting in %d seconds...", reconnect_delay)
    time.sleep(reconnect_delay)
    try:
      client.reconnect()
      log.info("Reconnected successfully!")
      return
    except Exception as err:
      log.error("%s. Reconnect failed. Retrying...", err)
    
    reconnect_delay *= RECONNECT_RATE
    reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
    reconnect_count += 1
  log.critical("Reconnect failed after %s attempts. Exiting...", reconnect_count)
  client.loop_stop()
  sys.exit(1)

#########################################################################################################################################################

conn_controller = threading.Thread(target=connectionCheck, args=(options.serialDevice, 1,))
conn_controller.setDaemon(True)
conn_controller.start()

client = mqtt.Client(options.username)
client.username_pw_set(options.username, options.password)
client.enable_logger(log)
#client.will_set(telemetry, payload=None, qos=1, retain=True)
#client.reconnect_delay_set(min_delay=1, max_delay=120)
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish
client.on_subscribe = on_subscribe
client.on_disconnect = on_disconnect
client.connect(options.host, mqttPort, 60)
client.loop_start()
#client.publish(telemetry, '{"tester":"84834"}',qos=1,retain=True)

if __name__ == '__main__':
  main()
