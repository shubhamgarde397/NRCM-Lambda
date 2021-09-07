import json
from bson import ObjectId
import bson
import pymongo
from pymongo import MongoClient
import os
import math
from datetime import datetime
import consts
import functions
import re
import turnbook
import insert
import connection
import dynamicConstants
import partyPayment

def data(event):
    method = dynamicConstants.dynamicC(event,'method')
    many = dynamicConstants.dynamicC(event,'many')
    insider = dynamicConstants.dynamicC(event,'insider')
    username = dynamicConstants.dynamicC(event,'username')
    turnbookdate = dynamicConstants.dynamicC(event,'turnbookdate')
    partyid = dynamicConstants.dynamicC(event,'partyid')
    fromDate = dynamicConstants.dynamicC(event,'fromDate')
    toDate = dynamicConstants.dynamicC(event,'toDate')
    balanceHireDisplayType = dynamicConstants.dynamicC(event,'balanceHireDisplayType')
    turnbookUpdateNumber = dynamicConstants.dynamicC(event,'turnbookUpdateNumber')
    paymentpipeline=consts.paymentpipeline
    paymentpipeline.insert(0,{'$match': {'$and': [{'date': {'$gte': fromDate, '$lte': toDate}}, {'partyid': partyid}]}})
    tables = consts.tables
    arr = consts.arr
    i = consts.i
    roleI = consts.roleI
    typeofdb = event['typeofuser']
    user=event['user']
    tempResponse = []
    tableName = event['tablename'] if (method!='display') and (method!='displaynew') else ''
    
    # cluster = connection.cluster
    # db = connection.db
    
    cluster = connection.cluster(functions.connectionString(event['typeofuser']))
    db = connection.db(event['typeofuser'])
    
    if insider == 'new':
        event['datetruck'] = event['turnbookDate']+'_'+event['truckno']
        try:
            newevent={"truckno": event['truckno'],"reference": [],"accountDetails": [],"personalDetails": "","oname":"","pan":"","reference":"","contact":[],"preferences":[]}
            _id = db['ownerdetails'].insert_one(functions.insertOrUpdate('ownerdetails',newevent,'insert'))
            truckid = json.loads(json.dumps(functions.responser(event,'ownerdetails',_id), default=functions.json_unknown_type_handler))
            event['ownerid']=truckid['_id']
        except pymongo.errors.DuplicateKeyError:
            tempResponse = functions.responser('','','')
            tempResponse = functions.responser('','ignore','')
            a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
            return a
        else:
            try:
                _id = db[functions.createColName(tableName,event,['01','02','03'])].insert_one(functions.insertOrUpdate(tableName,event,'insert'))
                tempResponse = functions.responser(event,tableName,_id)
                print(tempResponse)
                a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
                print(a)
                if insider == 'new':
                    a['_id'] += '+'+event['ownerid'] 
                return a
            except pymongo.errors.DuplicateKeyError:
                tempResponse = functions.responser('','','')
                a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
                if insider == 'new':
                    a['_id'] += '+'+event['ownerid'] 
                return a
                
    if method == 'displaynew':
        role=event['consider'][0]
        C=event['consider']
        for io in range(0,len(C)-1):
            tables[io]['consider']=C[io+1]
        newarr=functions.considerArrayData(event,db,tables,roleI,role,username)
        return json.loads(json.dumps(newarr,default=functions.json_unknown_type_handler))
    
        
    elif method == 'BalanceHireDisplay':
        col=db['BalanceHire']
        arr = {'Data':[],'Status':'BalanceHireDetails'}
        tp=[]
        if balanceHireDisplayType=='year':
            res=col.aggregate([{'$match': {'todayDate': re.compile(event['createdDate'])}}, {'$addFields': {'month': {'$arrayElemAt': [{'$split': ['$todayDate', '-']}, 1]}}}, {'$group': {'_id': {'$convert': {'input': '$month', 'to': 'int'}}, 'data': {'$sum': 1}, 'print': {'$addToSet': '$print'}}}, {'$project': {'data': 1, '_id': 1, 'print': {'$cond': {'if': {'$eq': [{'$size': ['$print']}, 1]}, 'then': {'$arrayElemAt': ['$print', 0]}, 'else': False}}}}, {'$sort': {'_id': 1}}, {'$addFields': {'name': {'$let': {'vars': {'monthsinString': ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']}, 'in': {'$arrayElemAt': ['$$monthsinString', '$_id']}}}}}])
            for i in res:
                arr['Data'].append(i)
        elif balanceHireDisplayType=='month':
            res=col.aggregate([{'$match': {'todayDate': {'$gte': event['createdDate']+'-01', '$lte': event['createdDate']+'-31'}}}, {'$group': {'_id': '$todayDate', 'data': {'$sum': 1}, 'print': {'$first': '$print'}}}, {'$sort': {'_id': 1}}])
            for i in res:
                tp.append(i)
            arr['Data']=functions.completeTheMonth(tp)
        else:
            res=col.find({'todayDate': {'$eq': event['createdDate']}})
            for i in res:
                arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
        
    elif method == 'update':
        if turnbookUpdateNumber == 1:
            try:
                res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({'_id': ObjectId(event['_id'])}, {'$set': functions.insertOrUpdate(tableName,event,'update')})
                return {'Status': "Update",'Message':'Updated'}
            except pymongo.errors.DuplicateKeyError:
                return {'Status': "Update",'Message':"Duplicate Found"}
        elif turnbookUpdateNumber == 2:
            try:
                res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({'_id': ObjectId(event['_id'])}, {'$set': functions.insertOrUpdate(tableName,event,'update')})
                return {'Status': "Update",'Message':'Updated'}
            except pymongo.errors.DuplicateKeyError:
                return {'Status': "Update",'Message':"Duplicate Found"}
        elif turnbookUpdateNumber == 3:
            try:
                res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({'_id': ObjectId(event['_id'])}, {'$set': functions.insertOrUpdate(tableName,event,'update')})
                return {'Status': "Update",'Message':'Updated'}
            except pymongo.errors.DuplicateKeyError:
                return {'Status': "Update",'Message':"Duplicate Found"}
        else:
            if event['print']:
                try:
                    res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_many({'todayDate':event['todayDate']}, {'$set': {'print':event['print']}})
                    return {'Status': "Update",'Message':'Updated'}
                except pymongo.errors.DuplicateKeyError:
                    return {'Status': "Update",'Message':"Duplicate Found"}
            else:
                try:
                    res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({'_id': ObjectId(event['_id'])}, {'$set': functions.insertOrUpdate(tableName,event,'update')})
                    return {'Status': "Update",'Message':'Updated'}
                except pymongo.errors.DuplicateKeyError:
                    return {'Status': "Update",'Message':"Duplicate Found"}
                    
    elif method == 'displayTB':
        pipelineTurn=turnbook.pipelineValue(event,turnbookdate)
        col=db[functions.createColName(tableName,event,['01','02','03'])]
        arr = {'Data':[],'Status':'DisplayTB'}
        res=col.aggregate(pipelineTurn)
        for i in res:
            arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    
    elif method == 'insert':
        if user == 'anil':
            tempResponse = functions.responser('','ignore','')
            a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
            return a
        else:
            try:
                _id = db[functions.createColName(tableName,event,['01','02','03'])].insert_one(functions.insertOrUpdate(tableName,event,'insert'))
                tempResponse = functions.responser(event,tableName,_id)
                print(tempResponse)
                a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
                print(a)
                if insider == 'new':
                    a['_id'] += '+'+event['ownerid'] 
                return a
            except pymongo.errors.DuplicateKeyError:
                tempResponse = functions.responser('','','')
                a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
                if insider == 'new':
                    a['_id'] += '+'+event['ownerid'] 
                return a
                
    elif method == 'delete':
        res1 = db[functions.createColName(tableName,event,['01','02','03'])].delete_one({'_id': ObjectId(event['_id'])})
        return {'Status': "Delete"}
        
    elif method == 'updatePoch':#setting poch Payment to false
        res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({'_id': ObjectId(event['_id'])}, {'$set': {'pochPayment':False,'pochDate':''}})
        return {'Status': "Update",'Message':'Updated'}
    
    elif method == 'insertmany':
        if user == 'anil':
            tempResponse = functions.responser('','ignore','')
            a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
            return a
        else:
            try:
                _id = db[functions.createColName(tableName,event,['01','02','03'])].insert_many(functions.insertOrUpdate(tableName,event,'insert',many))
                tempResponse = functions.responser(event,tableName,_id,many)
                a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
                if many=='many':
                    res1 = db[functions.createColName('turnbook',event,['01','02','03'])].update_many({'_id':{'$in':list(map(ObjectId,event['ids']))}}, {'$set': {'pochPayment':True,'pochDate':event['todayDate']}})
                    
                return a
            except pymongo.errors.DuplicateKeyError:
                tempResponse = functions.responser('','','')
                a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
                return a
                
    elif method == 'displayPP':
        col=db[functions.createColName(tableName,event,['01','02','03'])]
        arr = {'Data':[],'Status':'DisplayPP'}
        res=col.aggregate(functions.getPaymentPipeline(event))
        for i in res:
            arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    elif method == 'partyPaymentPDF':
        arr = {'Data':[],'Status':'DisplayPP'}
        res1 = db['partyPayment'].aggregate(partyPayment.createPipeline(consts.pipelinePaymentPP1,'p1',event))
        res2 = db['TurnBook_2020_2021'].aggregate(partyPayment.createPipeline(consts.pipelinePaymentPP2,'p2',event))
        return json.loads(json.dumps(partyPayment.pipelineValue(event,res1,res2,arr), default=functions.json_unknown_type_handler))