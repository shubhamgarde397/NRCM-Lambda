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

def pipelineValue(event,turnbookdate):
    pipelineTurn = []
    if event['display'] == '0':
        pipelineTurn = consts.pipelineTurnS14
    elif event['display'] == '14':
        pipelineTurn = consts.pipelineTurnLocation
    else:
        pipelineTurn = consts.pipelineTurn
    try:
        a=pipelineTurn[0]['$match']
        pipelineTurn.pop(0)
    except KeyError:
        pass
    if event['display'] == '0':
        pipelineTurn.insert(0,{'$match': {'$and': [{'turnbookDate': {'$gte': event['turnbookDateS14']}},{'turnbookDate': {'$lte':event['turnbookDate']}}]}})
    if event['display'] == '1':
        pipelineTurn.insert(0,{'$match': {'$and': [{'turnbookDate': {'$gte': turnbookdate}}, {'loadingDate': ''}]}})
    if event['display'] == '2':
        pipelineTurn.insert(0,{'$match': {'turnbookDate': {'$eq': turnbookdate}}})
    if event['display'] == '3':
        pipelineTurn.insert(0,{'$match': {'loadingDate': {'$eq': turnbookdate}}})
    if event['display'] == '5':
        pipelineTurn.insert(0,{'$match': {'$and': [{'loadingDate': {'$gte': turnbookdate+'-01', '$lte': turnbookdate+'-31'}}, {'partyType': 'NRCM'}]}})
    if event['display'] == '6':
        pipelineTurn.insert(0,{'$match': {'$and':[{'pochPayment': False},{'loadingDate':{'$ne':''}},{'partyType':{'$in':['','NRCM','NR']}}]}})
    if event['display'] == '7':
        pipelineTurn.insert(0,{'$match': {'loadingDate': re.compile(event['date']), 'pochPayment': True}})
    if event['display'] == '8':
        pipelineTurn = []
        pipelineTurn = functions.addMonthYear(event['date'],consts.pipelineTurnNew)
    if event['display'] == '9':
        pipelineTurn.insert(0,{'$match': {'partyType':'Cancel'}})
    if event['display'] == '10':
        pipelineTurn.insert(0,{'$match': {'partyid':event['partyid'],'loadingDate':{'$ne':""}}})
    if event['display'] == '12':
        pipelineTurn.insert(0,{'$match': {'invoice':event['invoice']}})
    if event['display'] == '13':
        pipelineTurn.insert(0,{'$match': {'lrno':event['lrno']}})
    if event['display'] == '14':
        # pipelineTurn=[{'$addFields': {'lkl': {'$last': '$locations'}}}, {'$match': {'$expr': {'$ne': ['$placeid', '$lkl']}, 'loadingDate': {'$nin': ['']}}}]
        pipelineTurn.insert(0,{'$match':{}})
    print(pipelineTurn)
    return pipelineTurn
    
def pipelineValueReport(event,turnbookdate):
    pipelineReport = []
    pipelineReport = consts.pipelineReport
    try:
        a=pipelineReport[0]['$match']
        pipelineReport.pop(0)
    except KeyError:
        pass
    pipelineReport.insert(0,{'$match':{'$and':[{'pochPayment':False},{'loadingDate':{'$lte':event['date']}}]}})
    return pipelineReport    
