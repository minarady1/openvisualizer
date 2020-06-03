# Copyright (c) 2010-2013, Regents of the University of California. 
# All rights reserved. 
#  
# Released under the BSD 3-Clause license as published at the link below.
# https://openwsn.atlassian.net/wiki/display/OW/License
import logging
log = logging.getLogger('ParserData')
log.setLevel(logging.ERROR)
log.addHandler(logging.NullHandler())

import struct

from pydispatch import dispatcher

from ParserException import ParserException
import Parser

import datetime
import paho.mqtt.client as mqtt
import threading
import json
import numpy as np

import networkInfo

def init_pkt_info():
    return {
                        'asn'            : 0,
                        'src_id'      : None,
                        'counter'        : 0,
                        'latency'        : 0,
                        'numCellsUsedTx' : 0,
                        'numCellsUsedRx' : 0,
                        'dutyCycle'      : 0
    }

def data_describe (data):
    mean= np.mean(data)
    median = np.median(data)
    std = np.std(data)
    first_q = np.quantile(data, 0.25)
    third_q = np.quantile(data, 0.75)   
    return mean,median,std,min(data),max(data),first_q,third_q;

class ParserData(Parser.Parser):
    
    HEADER_LENGTH  = 2
    MSPERSLOT      = 0.02 #second per slot.
    
    IPHC_SAM       = 4
    IPHC_DAM       = 0
    
    UINJECT_MASK    = 'uinject'
     
    def __init__(self, mqtt_broker_address):
        
        # log
        log.info("create instance")
        
        # initialize parent class
        Parser.Parser.__init__(self,self.HEADER_LENGTH)
        
        self._asn= ['asn_4',                     # B
          'asn_2_3',                   # H
          'asn_0_1',                   # H
         ]

        self.avg_kpi = {}
        self.first_arrival_ts = -1

        self.broker                    = mqtt_broker_address
        self.mqttconnected             = False

        if not (self.broker == 'null'):

             # connect to MQTT
            self.mqttclient                = mqtt.Client()
            self.mqttclient.on_connect     = self._on_mqtt_connect

            try:
                self.mqttclient.connect(self.broker)
            except Exception as e:
                log.error("Failed to connect to {} with error msg: {}".format(self.broker, e))
            else: 
                # start mqtt client
                self.mqttthread                = threading.Thread(
                    name                       = 'mqtt_loop_thread',
                    target                     = self.mqttclient.loop_forever
                )
                self.mqttthread.start()

     #======================== private =========================================

    def _on_mqtt_connect(self, client, userdata, flags, rc):

        log.info("Connected to MQTT")

        self.mqttconnected = True


    #======================== public ==========================================
    
    def parseInput(self,input):
        # log
        if log.isEnabledFor(logging.DEBUG):
            log.debug("received data {0}".format(input))
        
        # ensure input not short longer than header
        self._checkLength(input)
   
        headerBytes = input[:2]
        #asn comes in the next 5bytes.  
        
        asnbytes=input[2:7]
        (self._asn) = struct.unpack('<BHH',''.join([chr(c) for c in asnbytes]))
        
        #source and destination of the message
        dest = input[7:15]
        
        #source is elided!!! so it is not there.. check that.
        source = input[15:23]
        
        if log.isEnabledFor(logging.DEBUG):
            a="".join(hex(c) for c in dest)
            log.debug("destination address of the packet is {0} ".format(a))
        
        if log.isEnabledFor(logging.DEBUG):
            a="".join(hex(c) for c in source)
            log.debug("source address (just previous hop) of the packet is {0} ".format(a))
        
        # remove asn src and dest and mote id at the beginning.
        # this is a hack for latency measurements... TODO, move latency to an app listening on the corresponding port.
        # inject end_asn into the packet as well
        input = input[23:]
        
        if log.isEnabledFor(logging.DEBUG):
            log.debug("packet without source,dest and asn {0}".format(input))
        
        # when the packet goes to internet it comes with the asn at the beginning as timestamp.
         
        # cross layer trick here. capture UDP packet from udpLatency and get ASN to compute latency.
        offset  = 0
        if len(input) >37:
            offset -= 7
            if self.UINJECT_MASK == ''.join(chr(i) for i in input[offset:]):
                                
                pkt_info = init_pkt_info()

                pkt_info['counter']      = input[offset-2] + 256*input[offset-1]                   # counter sent by mote
                offset -= 2

                pkt_info['asn']          = struct.unpack('<I',''.join([chr(c) for c in input[offset-5:offset-1]]))[0]
                aux                      = input[offset-5:offset]                               # last 5 bytes of the packet are the ASN in the UDP latency packet
                diff                     = self._asndiference(aux,asnbytes)            # calculate difference 
                pkt_info['latency']      = diff                                        # compute time in slots
                offset -= 5
                
                pkt_info['numCellsUsedTx'] = input[offset-1]
                offset -=1

                pkt_info['numCellsUsedRx'] = input[offset-1]
                offset -=1

                pkt_info['numNeighbors'] = input[offset-1]
                offset -=1

                pkt_info['src_id']       = ''.join(['%02x' % x for x in [input[offset-1],input[offset-2]]]) # mote id
                src_id                   = pkt_info['src_id']
                offset -=2

                numTicksOn               = struct.unpack('<I',''.join([chr(c) for c in input[offset-4:offset]]))[0]
                offset -= 4

                numTicksTx               = struct.unpack('<I',''.join([chr(c) for c in input[offset-4:offset]]))[0]
                offset -= 4

                numTicksRx               = struct.unpack('<I',''.join([chr(c) for c in input[offset-4:offset]]))[0]
                offset -= 4

                numTicksInTotal          = struct.unpack('<I',''.join([chr(c) for c in input[offset-4:offset]]))[0]
                offset -= 4

                pkt_info['dutyCycle']    = float(numTicksOn)/float(numTicksInTotal)    # duty cycle
                pkt_info['dutyCycleTx']  = float(numTicksTx)/float(numTicksInTotal)    # duty cycle
                pkt_info['dutyCycleRx']  = float(numTicksRx)/float(numTicksInTotal)    # duty cycle
                pkt_info['dutyCycleTxRx']= (float(numTicksTx)+float(numTicksRx))/float(numTicksInTotal)    # duty cycle

                #print pkt_info
                with open('pkt_info.log'.format(),'a') as f:
                    f.write(str(pkt_info)+'\n')
                
                if (pkt_info['dutyCycleRx']>1 or pkt_info['dutyCycleTxRx']>1):
                    print '>>>>>>>><<<<<<<<<<'
                    print numTicksOn
                    print numTicksTx
                    print numTicksRx
                    print numTicksInTotal

                    print pkt_info
                    print '>>>>>>>><<<<<<<<<<'
                
                # self.avg_kpi:
                if src_id in self.avg_kpi:
                    self.avg_kpi[src_id]['counter'].append(pkt_info['counter'])
                    self.avg_kpi[src_id]['latency'].append(pkt_info['latency'])
                    self.avg_kpi[src_id]['numCellsUsedTx'].append(pkt_info['numCellsUsedTx'])
                    self.avg_kpi[src_id]['numCellsUsedRx'].append(pkt_info['numCellsUsedRx'])
                    self.avg_kpi[src_id]['numNeighbors'].append(pkt_info['numNeighbors'])
                    self.avg_kpi[src_id]['dutyCycle'].append(pkt_info['dutyCycle'])
                    self.avg_kpi[src_id]['dutyCycleTx'].append(pkt_info['dutyCycleTx'])
                    self.avg_kpi[src_id]['dutyCycleRx'].append(pkt_info['dutyCycleRx'])
                    self.avg_kpi[src_id]['dutyCycleTxRx'].append(pkt_info['dutyCycleTxRx'])
                else:
                    self.avg_kpi[src_id] = {
                        'counter'        : [pkt_info['counter']],
                        'latency'        : [pkt_info['latency']],
                        'numCellsUsedTx' : [pkt_info['numCellsUsedTx']],
                        'numCellsUsedRx' : [pkt_info['numCellsUsedRx']],
                        'numNeighbors'   : [pkt_info['numNeighbors']],
                        'dutyCycle'      : [pkt_info['dutyCycle'] ],
                        'dutyCycleTx'    : [pkt_info['dutyCycleTx'] ],
                        'dutyCycleRx'    : [pkt_info['dutyCycleRx'] ],
                        'dutyCycleTxRx'  : [pkt_info['dutyCycleTxRx'] ],
                        'avg_cellsUsage' : 0.0,
                        'avg_latency'    : 0.0,
                        'avg_pdr'        : 0.0,
                        'avg_numNeighbors' : 0.0
                    }

                if self.mqttconnected:
                    self.publish_kpi(src_id)

                # in case we want to send the computed time to internet..
                # computed=struct.pack('<H', timeinus)#to be appended to the pkt
                # for x in computed:
                    #input.append(x)
            else:
                # no udplatency
                # print input
                pass     
        else:
            pass      
       
        eventType='data'
        # notify a tuple including source as one hop away nodes elide SRC address as can be inferred from MAC layer header
        return eventType, (source, input)

 #======================== private =========================================
 
    def _asndiference(self,init,end):
      
       asninit = struct.unpack('<HHB',''.join([chr(c) for c in init]))
       asnend  = struct.unpack('<HHB',''.join([chr(c) for c in end]))
       if asnend[2] != asninit[2]: #'byte4'
          return 0xFFFFFFFF
       else:
           pass
       
       return (0x10000*(asnend[1]-asninit[1])+(asnend[0]-asninit[0]))

#========================== mqtt publish ====================================

    def publish_kpi(self, src_id):

        payload = {
            'token':       123,
            'stats':{
                'avg_numNeighbors':{},
                'avg_cellsUsage':{},
                'avg_latency':{},
                'avg_dutyCycle':{},
                'avg_dutyCycleTx':{},
                'avg_dutyCycleRx':{},
                'avg_dutyCycleTxRx':{},
                'avg_pdr':{},
            }
        }
        

        mote_data = self.avg_kpi[src_id]
        self.avg_kpi[src_id]['avg_cellsUsage'] = float(sum(mote_data['numCellsUsedTx'])/len(mote_data['numCellsUsedTx']))/float(64)
        self.avg_kpi[src_id]['avg_latency']    = sum(self.avg_kpi[src_id]['latency'])/len(self.avg_kpi[src_id]['latency'])
        self.avg_kpi[src_id]['avg_dutyCycle']    = sum(self.avg_kpi[src_id]['dutyCycle'])/len(self.avg_kpi[src_id]['dutyCycle'])
        self.avg_kpi[src_id]['avg_dutyCycleTx']    = sum(self.avg_kpi[src_id]['dutyCycleTx'])/len(self.avg_kpi[src_id]['dutyCycleTx'])
        self.avg_kpi[src_id]['avg_dutyCycleRx']    = sum(self.avg_kpi[src_id]['dutyCycleRx'])/len(self.avg_kpi[src_id]['dutyCycleRx'])
        self.avg_kpi[src_id]['avg_dutyCycleTxRx']    = sum(self.avg_kpi[src_id]['dutyCycleTxRx'])/len(self.avg_kpi[src_id]['dutyCycleTxRx'])
        self.avg_kpi[src_id]['avg_numNeighbors']    = sum(self.avg_kpi[src_id]['numNeighbors'])/len(self.avg_kpi[src_id]['numNeighbors'])
        mote_data['counter'].sort() # sort the counter before calculating
        self.avg_kpi[src_id]['avg_pdr']        = float(len(set(mote_data['counter'])))/float(1+mote_data['counter'][-1]-mote_data['counter'][0])

        avg_pdr_all           = 0.0
        avg_latency_all       = 0.0
        avg_dutyCycle_all     = 0.0
        avg_dutyCycleTx_all   = 0.0
        avg_dutyCycleRx_all   = 0.0
        avg_dutyCycleTxRx_all = 0.0
        avg_numCellsUsage_all = 0.0
        avg_numNeighbors_all  = 0.0

        arr_avg_pdr_all           = []
        arr_avg_latency_all       = []
        arr_avg_dutyCycle_all     = []
        arr_avg_dutyCycleTx_all   = []
        arr_avg_dutyCycleRx_all   = []
        arr_avg_dutyCycleTxRx_all = []
        arr_avg_numCellsUsage_all = []
        arr_avg_numNeighbors_all  = []

        #print self.avg_kpi
        for mote, data in self.avg_kpi.items():
            avg_pdr_all           += data['avg_pdr']
            arr_avg_pdr_all.append (data['avg_pdr'])
            
            avg_latency_all       += data['avg_latency']
            arr_avg_latency_all.append (data['avg_latency'])
            
            avg_dutyCycle_all     += data['avg_dutyCycle']
            arr_avg_dutyCycle_all.append (data['avg_dutyCycle'])
            
            avg_dutyCycleTx_all     += data['avg_dutyCycleTx']
            arr_avg_dutyCycleTx_all.append (data['avg_dutyCycleTx'])

            avg_dutyCycleRx_all     += data['avg_dutyCycleRx']
            arr_avg_dutyCycleRx_all.append (data['avg_dutyCycleRx'])
     
            avg_dutyCycleTxRx_all     += data['avg_dutyCycleTxRx']
            arr_avg_dutyCycleTxRx_all.append (data['avg_dutyCycleTxRx'])

            avg_numCellsUsage_all += data['avg_cellsUsage']
            arr_avg_numCellsUsage_all.append (data['avg_cellsUsage'])
            
            avg_numNeighbors_all += data['avg_numNeighbors']
            arr_avg_numNeighbors_all.append (data['avg_numNeighbors'])
            

        numMotes = len(self.avg_kpi)
        avg_pdr_all                = avg_pdr_all/float(numMotes)
        avg_latency_all            = avg_latency_all/float(numMotes)
        avg_dutyCycle_all          = avg_dutyCycle_all/float(numMotes)
        avg_dutyCycleTx_all          = avg_dutyCycleTx_all/float(numMotes)
        avg_dutyCycleRx_all          = avg_dutyCycleRx_all/float(numMotes)
        avg_dutyCycleTxRx_all          = avg_dutyCycleTxRx_all/float(numMotes)
        avg_numCellsUsage_all      = avg_numCellsUsage_all/float(numMotes)
        avg_numNeighbors_all      = avg_numNeighbors_all/float(numMotes)

        mean,median,std,min_v,max_v,first_q,third_q = data_describe(arr_avg_numCellsUsage_all)
        payload['avg_cellsUsage'] = avg_numCellsUsage_all
        payload['stats']['avg_cellsUsage']['mean']     = mean
        payload['stats']['avg_cellsUsage']['median']   = median
        payload['stats']['avg_cellsUsage']['std']      = std
        payload['stats']['avg_cellsUsage']['min_v']    = min_v
        payload['stats']['avg_cellsUsage']['max_v']    = max_v
        payload['stats']['avg_cellsUsage']['first_q']  = first_q
        payload['stats']['avg_cellsUsage']['third_q']  = third_q
        
        mean,median,std,min_v,max_v,first_q,third_q = data_describe(arr_avg_latency_all)
        payload['avg_latency'] = avg_latency_all
        payload['stats']['avg_latency']['mean']     = mean
        payload['stats']['avg_latency']['median']   = median
        payload['stats']['avg_latency']['std']      = std
        payload['stats']['avg_latency']['min_v']    = min_v
        payload['stats']['avg_latency']['max_v']    = max_v
        payload['stats']['avg_latency']['first_q']  = first_q
        payload['stats']['avg_latency']['third_q']  = third_q
        
        mean,median,std,min_v,max_v,first_q,third_q = data_describe(arr_avg_dutyCycle_all)
        payload['avg_dutyCycle']   = avg_dutyCycle_all
        payload['stats']['avg_dutyCycle']['mean']     = mean
        payload['stats']['avg_dutyCycle']['median']   = median
        payload['stats']['avg_dutyCycle']['std']      = std
        payload['stats']['avg_dutyCycle']['min_v']    = min_v
        payload['stats']['avg_dutyCycle']['max_v']    = max_v
        payload['stats']['avg_dutyCycle']['first_q']  = first_q
        payload['stats']['avg_dutyCycle']['third_q']  = third_q
        
        mean,median,std,min_v,max_v,first_q,third_q = data_describe(arr_avg_dutyCycleTx_all)
        payload['avg_dutyCycleTx']   = avg_dutyCycleTx_all
        payload['stats']['avg_dutyCycleTx']['mean']     = mean
        payload['stats']['avg_dutyCycleTx']['median']   = median
        payload['stats']['avg_dutyCycleTx']['std']      = std
        payload['stats']['avg_dutyCycleTx']['min_v']    = min_v
        payload['stats']['avg_dutyCycleTx']['max_v']    = max_v
        payload['stats']['avg_dutyCycleTx']['first_q']  = first_q
        payload['stats']['avg_dutyCycleTx']['third_q']  = third_q
        
        mean,median,std,min_v,max_v,first_q,third_q = data_describe(arr_avg_dutyCycleRx_all)
        payload['avg_dutyCycleRx']   = avg_dutyCycleRx_all
        payload['stats']['avg_dutyCycleRx']['mean']     = mean
        payload['stats']['avg_dutyCycleRx']['median']   = median
        payload['stats']['avg_dutyCycleRx']['std']      = std
        payload['stats']['avg_dutyCycleRx']['min_v']    = min_v
        payload['stats']['avg_dutyCycleRx']['max_v']    = max_v
        payload['stats']['avg_dutyCycleRx']['first_q']  = first_q
        payload['stats']['avg_dutyCycleRx']['third_q']  = third_q
        
        mean,median,std,min_v,max_v,first_q,third_q = data_describe(arr_avg_dutyCycleTxRx_all)
        payload['avg_dutyCycleTxRx']   = avg_dutyCycleTxRx_all
        payload['stats']['avg_dutyCycleTxRx']['mean']     = mean
        payload['stats']['avg_dutyCycleTxRx']['median']   = median
        payload['stats']['avg_dutyCycleTxRx']['std']      = std
        payload['stats']['avg_dutyCycleTxRx']['min_v']    = min_v
        payload['stats']['avg_dutyCycleTxRx']['max_v']    = max_v
        payload['stats']['avg_dutyCycleTxRx']['first_q']  = first_q
        payload['stats']['avg_dutyCycleTxRx']['third_q']  = third_q
        
        mean,median,std,min_v,max_v,first_q,third_q = data_describe(arr_avg_pdr_all)
        payload['avg_pdr']   = avg_pdr_all
        payload['stats']['avg_pdr']['mean']     = mean
        payload['stats']['avg_pdr']['median']   = median
        payload['stats']['avg_pdr']['std']      = std
        payload['stats']['avg_pdr']['min_v']    = min_v
        payload['stats']['avg_pdr']['max_v']    = max_v
        payload['stats']['avg_pdr']['first_q']  = first_q
        payload['stats']['avg_pdr']['third_q']  = third_q    
        
        mean,median,std,min_v,max_v,first_q,third_q = data_describe(arr_avg_numNeighbors_all)
        payload['avg_numNeighbors']   = avg_numNeighbors_all
        payload['stats']['avg_numNeighbors']['mean']     = mean
        payload['stats']['avg_numNeighbors']['median']   = median
        payload['stats']['avg_numNeighbors']['std']      = std
        payload['stats']['avg_numNeighbors']['min_v']    = min_v
        payload['stats']['avg_numNeighbors']['max_v']    = max_v
        payload['stats']['avg_numNeighbors']['first_q']  = first_q
        payload['stats']['avg_numNeighbors']['third_q']  = third_q    


        payload['src_id']          = src_id

        #print payload
        payload ['rpl_node_count']=networkInfo.rpl_nodes_count
        payload ['rpl_churn']=networkInfo.rpl_churn
        delta= datetime.datetime.now() - networkInfo.set_root_timestamp
        payload ['time_elapsed']= {
        'seconds': delta.seconds, 
        'microseconds':delta.microseconds
        }

        if self.mqttconnected:
            # publish the cmd message
            self.mqttclient.publish(
                topic   = 'opentestbed/uinject/arrived/',
                payload = json.dumps(payload),
                qos=2
            )

