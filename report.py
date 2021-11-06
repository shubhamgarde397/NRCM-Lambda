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

def pipelineValue(event):
    pipelineTurn = []
    if event['option'] == 1:
        pipelineTurn = consts.pipelinePanDateWise
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'$and': [{'loadingDate': {'$gt': event['from']}}, {'loadingDate': {'$lt': event['to']}}]}} )
    if event['option'] == 2:#All
        pipelineTurn = consts.pipelinePanAll
        pipeMatchRemover(pipelineTurn)
    if event['option'] == 3:#By PartyType
        pipelineTurn = consts.pipelinePanTruckData
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'partyType':event['partyType']}} )
    if event['option'] == 4:#By PartyType AND dATE
        pipelineTurn = consts.pipelinePanTruckData
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'$and': [{'loadingDate': {'$gt': event['from']}}, {'loadingDate': {'$lt': event['to']}},{'partyType':event['partyType']}]}} )
    if event['option'] == 5:
        pipelineTurn=consts.pipelineAccountByDY
        pipeMatchRemover(pipelineTurn)
    if event['option'] == 6:#getting account and contact empty array
        pipelineTurn=consts.pipelineAccount
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'$and': [{'loadingDate': {'$gte': event['from']}}, {'loadingDate': {'$lte': event['to']}}]}})
    return pipelineTurn
    
def pipeMatchRemover(pipelineTurn):
    try:
        a=pipelineTurn[0]['$match']
        pipelineTurn.pop(0)
    except KeyError:
        pass
    
def pipelineDeepValue(event,type):
    pipelineTurn = []
    if type == 1:
        pipelineTurn = consts.pipelineDeep1
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'ownerid': event['ownerid']}})
    if type == 2:
        pipelineTurn = consts.pipelineDeep2
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'ownerid': event['ownerid']}})
    if type == 3:
        pipelineTurn = consts.pipelineDeep3
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'ownerid': event['ownerid']}})
    if type == 4:
        pipelineTurn = consts.pipelineDeep4
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'ownerid': event['ownerid']}})
    if type == 5:
        pipelineTurn = consts.pipelineDeep5
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'ownerid': event['ownerid']}})
    return pipelineTurn