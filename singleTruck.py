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
    print(event)
    pipelineTurn = []
    pipelineTurn = consts.singleTruckPipeline
    try:
        a=pipelineTurn[0]['$match']
        pipelineTurn.pop(0)
    except KeyError:
        pass
    if event['display'] == '11':
        pipelineTurn.insert(0,{'$match': {'ownerid': {'$in':event['realIds']}}})
    print(pipelineTurn)
    return pipelineTurn
