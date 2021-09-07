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
    pipelineTurn = consts.pipelineTurn
    try:
        a=pipelineTurn[0]['$match']
        pipelineTurn.pop(0)
    except KeyError:
        pass
    if event['display'] == '1':
        pipelineTurn.insert(0,{'$match': {'$and': [{'turnbookDate': {'$gte': turnbookdate}}, {'loadingDate': ''}]}})
    if event['display'] == '2':
        pipelineTurn.insert(0,{'$match': {'turnbookDate': {'$eq': turnbookdate}}})
    if event['display'] == '3':
        pipelineTurn.insert(0,{'$match': {'loadingDate': {'$eq': turnbookdate}}})
    if event['display'] == '5':
        pipelineTurn.insert(0,{'$match': {'$and': [{'loadingDate': {'$gte': turnbookdate+'-01', '$lte': turnbookdate+'-31'}}, {'partyType': 'NRCM'}]}})
    if event['display'] == '6':
        pipelineTurn.insert(0,{'$match': {'pochPayment': False}})
    if event['display'] == '7':
        pipelineTurn.insert(0,{'$match': {'loadingDate': re.compile(event['date']), 'pochPayment': True}})
    if event['display'] == '8':
        pipelineTurn = []
        pipelineTurn = functions.addMonthYear(event['date'],consts.pipelineTurnNew)
    if event['display'] == '9':
        pipelineTurn.insert(0,{'$match': {'partyType':'Cancel'}})
    return pipelineTurn
