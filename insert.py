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

def insertt(user,tableName,event,db,insider):
    if user == 'anil':
            tempResponse = functions.responser('','ignore','')
            a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
            return a
    else:
        try:
            _id = db[functions.createColName(tableName,event,['01','02','03'])].insert_one(functions.insertOrUpdate(tableName,event,'insert'))
            tempResponse = functions.responser(event,tableName,_id)
            a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
            if insider == 'new':
                a['_id'] += '+'+event['ownerid'] 
            return a
        except pymongo.errors.DuplicateKeyError:
            tempResponse = functions.responser('','','')
            a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
            if insider == 'new':
                a['_id'] += '+'+event['ownerid'] 
            return a