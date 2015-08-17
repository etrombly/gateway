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
NODEID = 1
KEY = "1234567891011121"
FREQ = RF69_915MHZ #options are RF69_915MHZ, RF69_868MHZ, RF69_433MHZ, RF69_315MHZ

class Message(object):
    def __init__(self, message = None):
        self.nodeID = 0
        self.devID = 0
        self.cmd = 0
        self.packetID = 0
        self.intVal = 0
        self.fltVal = 0.0
        self.payload = ""
        self.s = struct.Struct('<BBBBlf32s')
        self.message = message
        if message:
            self.getMessage()

    def setMessage(self, nodeID = None, devID = 1, cmd = 0, packetID = 0, intVal = 0, fltVal = 0.0, payload = ""):
        if nodeID:
            self.message = self.s.pack(nodeID, devID, cmd, packetID, intVal, fltVal, payload)
            self.nodeID, self.devID, self.cmd, self.packetID, self.intVal, self.fltVal, self.payload = nodeID, devID, cmd, packetID, intVal, fltVal, payload
        else:
            self.message = self.s.pack(self.nodeID, self.devID, self.cmd, self.packetID, self.intVal, self.fltVal, self.payload)

    def getMessage(self):
        try:
            self.nodeID, self.devID, self.cmd, self.packetID, self.intVal, self.fltVal, self.payload = \
                self.s.unpack_from(buffer(self.message))
            self.setMessage()
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

        self.messageBuffer =[]
        self.packetID = 1

    def incPacketID(self):
        if self.packetID < 255:
            self.packetID += 1
        else:
            self.packetID = 1
        return self.packetID

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
            message.packetID = self.incPacketID()
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
            self.messageBuffer.append([message, 4, int(time.time() * 1000)])

    def processPacket(self, packet, sender, ack):
        message = Message(packet)
        buff = None
        match = None

        #print "received packet nodeID:", message.nodeID, " packetID: ", message.packetID

        if ack:
            for packet in self.messageBuffer:
                if packet[0].packetID == message.packetID and packet[0].nodeID == message.nodeID:
                    match = packet
                    #print "ack recieved for", message.packetID

        if match:
            self.messageBuffer.remove(match)
            return

        self.sendAck(sender, message)

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
            #print "home/rfm_gw/nb/node%02d/dev%02d" % (message.nodeID, message.devID), buff
            self.mqttc.publish("home/rfm_gw/nb/node%02d/dev%02d" % (message.nodeID, message.devID), buff)

    def sendAck(self, sendTo, message):
        #print "Sending ack nodeID: ", message.nodeID, " packetID: ", message.packetID, " to ", sendTo
        self.radio.sendACK(sendTo, message.message)

    def processMessages(self):
        expired = []
        for message in self.messageBuffer:
            if message[1] < 1:
                expired.append(message)
                #if message[1] == 0:
                    #print message[0].nodeID, message[0].packetID, "not received"
                next
            if message[1] == 4 or int(time.time() * 1000) > message[2] + 100:
                #print "sending nodeID: ", message[0].nodeID, "packetID: ", message[0].packetID
                self.radio.send(message[0].nodeID, message[0].message)
                message[1] -= 1
                message[2] = int(time.time() * 1000)
        for message in expired:
            self.messageBuffer.remove(message)

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
            gw.processMessages()
            #time.sleep(.1)
        if gw.radio.ACK_RECEIVED:
            continue
        packet = bytearray(gw.radio.DATA)
        gw.processPacket(packet, gw.radio.SENDERID, gw.radio.ACK_RECEIVED)
