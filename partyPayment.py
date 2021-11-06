import pymongo
from pymongo import MongoClient
import json
from bson import ObjectId
import bson
import os
import math
from datetime import datetime
import consts
import functions
import re

def pipelineValue(event,res2,arr):
    arr1 = []
    arr2=[]
    # for i in res1:
    #     arr1.append(i)
    for i in res2:
        arr2.append(i)
    arr2.extend(arr1)
    arr2.sort(key=lambda x:x['date'])
    for i in arr2:
        arr['Data'].append(i)
    return arr

def createPipeline(data,types,event):
    pipelineTurn = data
    try:
        a=pipelineTurn[0]['$match']
        pipelineTurn.pop(0)
    except KeyError:
        pass
    # if types == 'p1':
    #     pipelineTurn.insert(0,{'$match': {'$and': [{'date': {'$gte': event['from'], '$lte': event['to']}},{'partyid': {'$in':event['partyid']}}]}})
    if types == 'p2':
        pipelineTurn.insert(0,{'$match': {'$and': [{'loadingDate': {'$gte': event['from'], '$lte': event['to']}}, {'partyid': {'$in':event['partyid']}}]}})
    return pipelineTurn
    
def pipelineValueForParty(event,res1,res2,arr):
    arr1 = []
    arr2=[]
    for i in res1:
        arr1.append(i)
    for i in res2:
        arr2.append(i)
    arr2.extend(arr1)
    arr2.sort(key=lambda x:x['date'])
    for i in arr2:
        arr['Data'].append(i)
    return arr

def createPipelineForParty(data,types,event):
    pipelineTurn = data
    try:
        a=pipelineTurn[0]['$match']
        pipelineTurn.pop(0)
    except KeyError:
        pass
    if types == 'p1':
        pipelineTurn.insert(0,{'$match': {'$and': [{'date': {'$gte': event['frompayment'], '$lte': event['topayment']}}, {'partyid': {'$in':event['partyid']}}]}})
    if types == 'p2':
        pipelineTurn.insert(0,{'$match': {'$and': [{'loadingDate': {'$gte': event['fromloading'], '$lte': event['toloading']}}, {'partyid': {'$in':event['partyid']}}]}})
    return pipelineTurn
