#{'$match': {'$and': [{'loadingDate': {'$gt': '2021-08-01'}}, {'loadingDate': {'$lt': '2021-08-31'}}], 'partyType': 'NRCM'}}, 

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
    if event['type'] == 'byMonth':
        pipelineTurn = consts.pipelineChartByYear
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'$and': [{'loadingDate': {'$gt': event['from']}}, {'loadingDate': {'$lt': event['to']}}], 'partyType': 'NRCM'}}, )
    if event['type'] == 'byPartyYearwise':
        pipelineTurn = consts.pipelineChartByPartyYearWise
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'partyid': event['id'], 'partyType': 'NRCM', '$and': [{'loadingDate': {'$gt': event['from']}}, {'loadingDate': {'$lt': event['to']}}]}})
    if event['type'] == 'bySelectedPartyYearwise':
        pipelineTurn = consts.pipelineChartBySelectedPartyYearwise
        pipeMatchRemover(pipelineTurn)
        pipelineTurn.insert(0,{'$match': {'partyid': {'$in': event['id']}, 'partyType': 'NRCM', '$and': [{'loadingDate': {'$gt': event['from']}}, {'loadingDate': {'$lt': event['to']}}]}})
    return pipelineTurn
    
def pipeMatchRemover(pipelineTurn):
    try:
        a=pipelineTurn[0]['$match']
        pipelineTurn.pop(0)
    except KeyError:
        pass