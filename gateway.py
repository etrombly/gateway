#!/usr/bin/env python2

from RFM69 import RFM69
from RFM69.RFM69registers import *
import paho.mqtt.client as mqtt
import struct
import sys
import signal
import time
import Queue

VERSION = "2.1"
NETWORKID = 1
KEY = "1234567891011121"
FREQ = RF69_915MHZ #options are RF69_915MHZ, RF69_868MHZ, RF69_433MHZ, RF69_315MHZ
writeQ = Queue.Queue()

class Message(object):
    def __init__(self, message = None):
        self.nodeID = 0
        self.devID = 0
        self.cmd = 0
        self.intVal = 0
        self.fltVal = 0.0
        self.payload = ""
        self.s = struct.Struct('<hhhlf32s')
        self.message = message
        if message:
            self.getMessage()
    
    def setMessage(self, nodeID = None, devID = None, cmd = 0, intVal = 0, fltVal = 0.0, payload = ""):
        if nodeID:
            self.message = self.s.pack(nodeID, devID, cmd, intVal, fltVal, payload)
        else:
            self.message = self.s.pack(self.nodeID, self.devID, self.cmd, self.intVal, self.fltVal, self.payload)
        self.getMessage()
        
    def getMessage(self):
        try:
            self.nodeID, self.devID, self.cmd, self.intVal, self.fltVal, self.payload = \
                self.s.unpack_from(buffer(self.message))
        except:
            print "could not extract message"

class Gateway(object):
    def __init__(self, freq, networkID, key):
        self.mqttc = mqtt.Client()
        self.mqttc.on_connect = self.mqttConnect
        self.mqttc.on_message = self.mqttMessage
        self.mqttc.connect("127.0.0.1", 1883, 60)
        self.mqttc.loop_start()
        print "mqtt init complete"
        
        self.radio = RFM69.RFM69(freq, 1, networkID, True)
        self.radio.rcCalibration()
        self.radio.encrypt(key)
        print "radio init complete"
    
    def receiveBegin(self):
        self.radio.receiveBegin()
    
    def receiveDone(self):
        return self.radio.receiveDone()
    
    def mqttConnect(self, client, userdata, flags, rc):
        self.mqttc.subscribe("home/rfm_gw/sb/#")
    
    def mqttMessage(self, client, userdata, msg):
        message = Message()
        if len(msg.topic) == 27:
            message.nodeID = int(msg.topic[19:21])
            message.devID = int(msg.topic[25:27])
            message.payload = str(msg.payload)
            if message.payload == "READ":
                message.cmd = 1
            
            statMess = message.devID in [5, 6, 8] + range(16, 31)
            realMess = message.devID in [0, 2, 3, 4] + range(40, 71) and message.cmd == 1
            intMess = message.devID in [1, 7] + range(32, 39)
            strMess = message.devID == 72
            
            if message.nodeID == 1:
                if message.devID == 0:
                    try:
                        with open('/proc/uptime', 'r') as uptime_file:
                            uptime = int(float(uptime_file.readline().split()[0]) / 60)
                    except:
                        uptime = 0
                    self.mqttc.publish("home/rfm_gw/nb/node01/dev00", uptime)
                elif message.devID == 3:
                    self.mqttc.publish("home/rfm_gw/nb/node01/dev03", VERSION)
                return
            else:
                if statMess:
                    if message.payload == "ON":
                        message.intVal = 1
                    elif message.payload == "OFF":
                        message.intVal = 0
                    else:
                        #invalid status command
                        self.error(3, message.nodeID)
                        return
                elif realMess:
                    try:
                        message.fltVal = float(message.payload)
                    except:
                        pass
                elif intMess:
                    if message.cmd == 0:
                        message.intVal = int(message.payload)
                elif strMess:
                    pass
                else:
                    #invalid devID
                    self.error(4, message.nodeID)
                    return
            
            message.setMessage()
            writeQ.put(message)
        
    def processPacket(self, packet):
        message = Message(packet)
        buff = None
        
        statMess = message.devID in [5, 6, 8] + range(16, 31)
        realMess = message.devID in [4] + range(48, 63) and message.cmd == 1
        intMess = message.devID in [0, 1, 2, 7] + range(16, 31)
        strMess = message.devID in [3, 72]
        
        if intMess:
            buff = "%d" % (message.intVal, )
        if realMess:
            buff = "%.2d" % (message.fltVal, )
        if statMess:
            if message.intVal == 1:
                buff = "ON"
            elif message.intVal == 0:
                buff = "OFF"
        if strMess:
            buff = message.payload
        if message.devID in range(40, 47):
            buff = "Binary input activated"
        elif message.devID == 92:
            buff = "NODE %d invalid device %d" % (message.nodeID, message.intVal)
        elif message.devID == 99:
            buff = "NODE %d WAKEUP" % (message.nodeID, )
        
        if buff:
            self.mqttc.publish("home/rfm_gw/nb/node%02d/dev%02d" % (message.nodeID, message.devID), buff)
    
    def sendMessage(self, message):
        if not self.radio.sendWithRetry(message.nodeID, message.message, 5, 30):
            self.mqttc.publish("home/rfm_gw/nb/node%02d/dev90" % (message.nodeID, ), 
                               "connection lost node %d" % (message.nodeID))
    
    def error(self, code, dest):
        self.mqttc.publish("home/rfm_gw/nb/node01/dev91", "syntax error %d for node %d" % (code, dest))

    def stop(self):
        print "shutting down mqqt"
        self.mqttc.loop_stop()
        print "shutting down radio"
        self.radio.shutdown()

def handler(signum, frame):
    print "\nExiting..."
    gw.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, handler)

gw = Gateway(FREQ, NETWORKID, KEY)

if __name__ == "__main__":
    while True:
        gw.receiveBegin()
        while not gw.receiveDone():
            try:
                message = writeQ.get(block = False)
                gw.sendMessage(message)
            except Queue.Empty:
                pass
        if gw.radio.ACK_RECEIVED:
            continue
        packet = bytearray(gw.radio.DATA)
        if gw.radio.ACKRequested():
            gw.radio.sendACK()
        gw.processPacket(packet)
