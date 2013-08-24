from py2neo import *

from Condition import *

import math
import numpy
import abc

class Condition_distance(Condition):
    def execute(self,db,input):
        #input struct: [type,switchList,hostList,hdd_size,ram,n_vcpu,network_id,weights,h]
        type = input[0]
        switchList = input[1]
        hostList = input[2]
        hdd_size = input[3]
        ram = input[4]
        n_vcpu = input[5]
        network_id = input[6]
        weights = input[7]
        thisHost = input[8]
        
        # get existing instances' host information
        instanceHostList = []
        instanceIndex = db.get_index(neo4j.Node,"instance")
        if instanceIndex != None:
        	instanceNodeList = instanceIndex.query("*:*")
        	for instanceNode in instanceNodeList:
        		hostNode = instanceNode.get_related_nodes(0,"launched_on")
        		instanceHostList.append(hostNode[0])
        	
        # get exiting volumes' host information
        volumeHostList = []
        volumeIndex = db.get_index(neo4j.Node,"volume")
        if volumeIndex != None:
        	volumeNodeList = volumeIndex.query("*:*")
        	for volumeNode in volumeNodeList:
        		hostNode = volumeNode.get_related_nodes(0,"launched_on")
        		volumeHostList.append(hostNode[0])
        
        # calculate overall distance among virtual devices and hosts
        distanceScoreList = []
        if type == 0:
        	for thisHost in hostList:
        		distance2AllInstances = getHost2VdevDistance(instanceHostList,thisHost)
        		distanceScoreList.append(distance2AllInstances)
        else:
        	for thisHost in hostList:
        		distance2AllVolumes = getHost2VdevDistance(volumeHostList,thisHost)
        		distanceScoreList.append(distance2AllVolumes)
        
        # normalization
        distanceScoreList = normalize(distanceScoreList)
        
        return distanceScoreList
        

def getHost2VdevDistance(vdevHostList,thisHost):
	distance = 0
	for vdevHost in vdevHostList:
		distance += getHost2HostDistance(thisHost["name"],vdevHost["name"])
	return distance

def getHost2HostDistance(host1,host2):
	if host1["name"] == host2["name"]:
		return 0
	else:
		return 1
		# return minSeperation(host1,host2)


def normalize(scoreList):
	normalizedScoreList = []
	total = sum(scoreList)
	for n in range(len(scoreList)):
		normalizedScoreList.append(scoreList[n] / total)
	return normalizedScoreList
	
