# Copyright (c) 2010-2013, Regents of the University of California. 
# All rights reserved. 
#  
# Released under the BSD 3-Clause license as published at the link below.
# https://openwsn.atlassian.net/wiki/display/OW/License
'''
Module which receives DAO messages and calculates source routes.

.. moduleauthor:: Xavi Vilajosana <xvilajosana@eecs.berkeley.edu>
                  January 2013
.. moduleauthor:: Thomas Watteyne <watteyne@eecs.berkeley.edu>
                  April 2013
'''
import logging
log = logging.getLogger('topology')
log.setLevel(logging.ERROR)
log.addHandler(logging.NullHandler())

import threading
import time
from datetime import timedelta
import networkInfo

import openvisualizer.openvisualizer_utils as u
from openvisualizer.eventBus import eventBusClient

boxes = {
"B61C":"12",
"B640":"12",
"B53D":"12",
"B5B5":"12",
"B649":"13",
"B5E9":"13",
"B61F":"13",
"B624":"13",
"B5A9":"11",
"B58C":"11",
"B5F8":"11",
"B557":"11",
"B647":"20",
"B57E":"20",
"B618":"20",
"B5F2":"20",
"B595":"8",
"B5F3":"8",
"B5E7":"9",
"B5D8":"9",
"B55B":"9",
"B558":"9",
"B58A":"6",
"B57D":"6",
"B5FA":"6",
"B5D2":"6",
"B4D1":"18",
"B579":"18",
"B628":"18",
"B54F":"18",
"B5DB":"2",
"B5E4":"2",
"B545":"2",
"B638":"14",
"B5BF":"14",
"B588":"14",
"B5A3":"14",
"B5F1":"17",
"B48D":"17",
"B646":"17",
"B602":"17",
"B462":"5",
"B4AA":"5",
"B5B7":"5",
"B63A":"5",
"B629":"16",
"B593":"16",
"B498":"16",
"B563":"16",
"B597":"15",
"B57B":"15",
"B5C4":"15",
"B62B":"15",
"B5FB":"10",
"B5E5":"10",
"B5ED":"10",
"B578":"10",
"B605":"3",
"B571":"3",
"B612":"3",
"B622":"3",
"B61A":"19",
"B60B":"19",
"B565":"19",
"B648":"19"
}


rooms= {"1":"A125",
"2":"C234",
"3":"A126",
"5":"A112",
"6":"C403",
"7":"C0",
"8":"A102",
"9":"A102",
"10":"C5",
"11":"A120",
"12":"A116",
"13":"C117",
"14":"A102",
"15":"C113",
"16":"A107",
"17":"A124",
"18":"C014",
"19":"A102",
"20":"A115"}

class topology(eventBusClient.eventBusClient):
    
    def __init__(self):
        
        # local variables
        self.dataLock        = threading.Lock()
        self.parents         = {}
        self.parentsRadios   = {}
        self.parentsLastSeen = {}
        self.topologyNodes = []
        self.topologyEdges = []
        self.topologyNodesPrevious = []
        self.topologyEdgesPrevious = []
        self.NODE_TIMEOUT_THRESHOLD = 900
        
        eventBusClient.eventBusClient.__init__(
            self,
            name                  = 'topology',
            registrations         =  [
                {
                    'sender'      : self.WILDCARD,
                    'signal'      : 'updateParents',
                    'callback'    : self.updateParents,
                },
                {
                    'sender'      : self.WILDCARD,
                    'signal'      : 'getParents',
                    'callback'    : self.getParents,
                },
            ]
        )
    
    #======================== public ==========================================
    
    def getParents(self,sender,signal,data):
        return self.parents
    
    def getDAG(self):
        self.topologyNodesPrevious = self.topologyNodes
        self.topologyEdgesPrevious = self.topologyEdges

        states = []
        edges = []
        motes = []
        
        with self.dataLock:
            index =0
            for src, dsts in self.parentsRadios.iteritems():
                src_s = ''.join(['%02X' % x for x in src[-2:] ])
                motes.append(src_s)
                # bad programming: there should be more than once destination in dsts.
                # this is a hotfix now just to show the radio
                dst_s = ''.join(['%02X' % x for x in dsts["parent"][0][-2:] ])
                edges.append({ 'u':src_s, 'v':dst_s,  "value":{'label':networkInfo.radioSettingAbbreviationMap[dsts["radio"]]}})
                motes.append(dst_s)
            motes = list(set(motes))
            for mote in motes:
                box = boxes.get(mote)
                room = rooms.get(box)
                if (box and room):
                    d = { 'id': mote, 'value': { 'label': mote+'-'+room} }
                else:
                    d = { 'id': mote, 'value': { 'label': mote} }

                states.append(d)
        self.topologyNodes = states
        self.topologyEdges = edges
        return states, edges
        
    def updateParents(self,sender,signal,data):
        ''' inserts parent information into the parents dictionary '''
        with self.dataLock:
            #data[0] == source address, data[1] == list of parents, data[2] == list of Parents Radios
            self.parents.update({data[0]:data[1]})
            self.parentsRadios.update({data[0]: {
                'parent': data[1],
                'radio' : data[2]
                }})
            self.parentsLastSeen.update({data[0]: time.time()})

        self._clearNodeTimeout()

    def _clearNodeTimeout(self):

        threshold = time.time() - self.NODE_TIMEOUT_THRESHOLD
        with self.dataLock:
            for node in self.parentsLastSeen.keys():
                if self.parentsLastSeen[node] < threshold:
                    if node in self.parents:
                        del self.parents[node]
                    del self.parentsLastSeen[node]

    def computeChurn(self):
        #churn = nodes left + nodes joined+ nodes who changed parents (parent is known in both cases)
        #      = symmetric node set difference + churning edges count 
        tree1_nodes = []
        tree1_edges = {}
        
        tree2_nodes = []
        tree2_edges = {}

        churn = 0
        print ">>> >>> >>> CHURN"
        print len (self.topologyEdges)
        print len (self.topologyNodes)
        print len (self.topologyEdgesPrevious)
        print len (self.topologyNodesPrevious)

        if (len (self.topologyEdges)>1 and  len (self.topologyNodes)>1 and  len (self.topologyNodesPrevious)>1 and  len (self.topologyEdgesPrevious)>1):
            for x in self.topologyNodesPrevious:
                tree1_nodes.append(x['id'])
            
            for x in self.topologyNodes:
                tree2_nodes.append(x['id'])
            
            for x in self.topologyEdgesPrevious:
                tree1_edges [x['u']]= x['v']

            for x in self.topologyEdges:
                tree2_edges [x['u']]= x['v']

            diff = set(tree1_nodes) ^ set (tree2_nodes)
            #print diff
            #print len(diff)
            
            churn = len (diff)

            intersect = set(tree1_nodes) & set (tree2_nodes)
            
            for x in intersect:
                p1 = tree1_edges.get(x,"NONE")
                p2 = tree2_edges.get(x,"NONE")
                #print "-->"+x
                if (p1!=p2 and (p1!="NONE") and (p2!="NONE")):
                    churn+=1
        print churn
        return churn
    
    #======================== private =========================================
    
    #======================== helpers =========================================
