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
import charts
import report
import singleTruck
#
#
#
#Very impornatn, I have changed the dynamic Constants
#And Angular turnbook-main-display line no 
#
#
#

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
    addtotbids = dynamicConstants.dynamicC(event,'addtotbids')
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
    cluster = connection.cluster(functions.connectionString(event['typeofuser'],event['user']),event['user'])
    db = connection.db(event['typeofuser'],event['user'])
    
    if addtotbids == True:
        #write query here to add tb: _id to PartyPayment tbids.
        res1 = db['partyPayment'].update_one({'_id': ObjectId(event['paymentid'])}, {'$push': {'tbids':event['_id']}})        
    
    if insider == 'new':
        event['datetruck'] = event['turnbookDate']+'_'+event['truckno']
        try:
            newevent={"truckno": event['truckno'],"reference": [],"accountDetails": [],"personalDetails": "","oname":"","pan":"","drivingLic":"","regCard":"","reference":"","contact":[],"preferences":[]}
            _id = db['ownerdetails'].insert_one(functions.insertOrUpdate('ownerdetails',newevent,'insert'))
            truckid = json.loads(json.dumps(functions.responser(event,'ownerdetails',_id), default=functions.json_unknown_type_handler))
            event['ownerid']=truckid['_id']
        except pymongo.errors.DuplicateKeyError:
            tempResponse = functions.responser('','','')
            # tempResponse = functions.responser('','ignore','')
            a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
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
                
    elif method == 'delete':
        res1 = db[functions.createColName(tableName,event,['01','02','03'])].delete_one({'_id': ObjectId(event['_id'])})
        return {'Status': "Delete"}
        
    elif method == 'updatePoch':#setting poch Payment to false
        res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({'_id': ObjectId(event['_id'])}, {'$set': {'pochPayment':False,'pochDate':'','pgno':int('999') }})
        return {'Status': "Update",'Message':'Updated'}
    
    elif method == 'insertmany':
        if user == 'anil':
            tempResponse = functions.responser('','ignore','')
            a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
            return a
        else:
            try:
                _id = db[functions.createColName(tableName,event,['01','02','03'])].insert_many(functions.insertOrUpdate(tableName,event,'insert',many))
                #This below code is to update the pageno in Turnbook table  while adding Balance Hire
                if tableName == 'BalanceHire':
                    for i in range(0,len(event['ids'])):
                        res1 = db[functions.createColName('turnbook',event,['01','02','03'])].update_many({'_id':ObjectId(event['ids'][i])}, {'$set': {'pgno':event['bhData'][0]['truckData'][i]['pageno']}})
                tempResponse = functions.responser(event,tableName,_id,many)
                a = json.loads(json.dumps(tempResponse, default=functions.json_unknown_type_handler))
                #The below code is to update the pochdate and pochPayment in Turnbook while adding Balance Hire
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
        # res1 = db['partyPayment'].aggregate(partyPayment.createPipeline(consts.pipelinePaymentPP1,'p1',event))
        res2 = db['TurnBook_2020_2021'].aggregate(partyPayment.createPipeline(consts.pipelinePaymentPP2,'p2',event))
        return json.loads(json.dumps(partyPayment.pipelineValue(event,res2,arr), default=functions.json_unknown_type_handler))
    elif method == 'partyPaymentPDFForParty':
        arr = {'Data':[],'Status':'DisplayPP'}
        res1 = db['partyPayment'].aggregate(partyPayment.createPipelineForParty(consts.pipelinePaymentPP1,'p1',event))
        res2 = db['TurnBook_2020_2021'].aggregate(partyPayment.createPipelineForParty(consts.pipelinePaymentPP2,'p2',event))
        return json.loads(json.dumps(partyPayment.pipelineValueForParty(event,res1,res2,arr), default=functions.json_unknown_type_handler))    
        
    elif method == 'chart':
        arr = {'Data':[],'Status':'Chart'}
        selectedPipeline=charts.pipelineValue(event)
        res1 = db['TurnBook_2020_2021'].aggregate(selectedPipeline)
        for i in res1:
            arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    elif method == 'BalanceHireSingleUpdate':
        data='truckData.'+event['index']+'.'
        res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({'_id': ObjectId(event['id'])}, {'$set': {data+'date':event['date'],data+'amount':event['amount'],data+'pageno':event['pageno'],data+'truckno':event['truckno']}})
        return {'Status': "Update",'Message':'Updated'}
    elif method == 'BalanceHireSingleUpdateToDelete':
        res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({"_id":ObjectId(event['id'])},{'$unset':{"truckData."+event['index']:1}})
        res2 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({"_id":ObjectId(event['id'])},{'$pull':{"truckData":None}})
        return {'Status': "Delete"}
    elif method == 'BalanceHireSingleUpdateToAdd':
        res1 = db[functions.createColName(tableName,event,['01','02','03'])].update_one({"_id":ObjectId(event['id'])},{'$addToSet':{"truckData":{"date":event['date'],"truckno":event['truckno'],"amount":event['amount'],"pageno":event['pageno']}}})
        if res1.modified_count==1:
            return {'Status': "Update",'Message':'Updated'}
        else:
            return {'Status': "Update",'Message':'Cannot add duplicate records'}
    elif method == 'receivedReport':
        arr = {'Data':[],'Status':'Chart'}
        selectedPipeline=turnbook.pipelineValueReport(event,'')
        res1 = db['TurnBook_2020_2021'].aggregate(selectedPipeline)
        for i in res1:
            arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    elif method == 'pipelinePan':
        arr = {'Data':[],'Status':'Chart'}
        selectedPipeline=report.pipelineValue(event)
        res1 = db['TurnBook_2020_2021'].aggregate(selectedPipeline)
        for i in res1:
            arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    elif method == "singleTruck":
        arr = {'Data':[],'Status':'SingleTruck'}
        selectedPipeline=singleTruck.pipelineValue(event)
        res1 = db['TurnBook_2020_2021'].aggregate(selectedPipeline)
        for i in res1:
            arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    elif method == "displayA14Days":
        pipelineTurn=turnbook.pipelineValue(event,'2021-04-01')
        col=db[functions.createColName(tableName,event,['01','02','03'])]
        arr = {'Data':[],'Status':'DisplayTB'}
        res=col.aggregate(pipelineTurn)
        for i in res:
            arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    elif method == 'displayMail':
        pipelineTurn=consts.pipelineMailDisplay
        col=db[functions.createColName(tableName,event,['01','02','03'])]
        arr = {'Data':[],'Status':'DisplayCommon'}
        res=col.aggregate(pipelineTurn)
        for i in res:
            arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    elif method == 'deepDetails1':
        arr = {'Data':{'Detail1':[],'Detail2':[],'Detail3':[],'Detail4':[],'Detail5':[]},'Status':'DisplayCommon'}
        objects=['','Detail1','Detail2','Detail3','Detail4','Detail5']
        for j in range(1,6):
            pipelineTurn=report.pipelineDeepValue(event,j)
            col=db[functions.createColName(tableName,event,['01','02','03'])]
            res=col.aggregate(pipelineTurn)
            for i in res:
                arr['Data'][objects[j]].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    elif method == 'deepDetails':
        arr = {'Detail1':[],'Detail2':[],'Detail3':[],'Detail4':[],'Detail5':[],'Status':'DisplayCommon1'}
        objects=['','Detail1','Detail2','Detail3','Detail4','Detail5']
        for j in range(1,6):
            pipelineTurn=report.pipelineDeepValue(event,j)
            col=db[functions.createColName(tableName,event,['01','02','03'])]
            res=col.aggregate(pipelineTurn)
            for i in res:
                arr[objects[j]].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
    elif method == 'PartyDeleteSingleId':
        res1 = db['partyPayment'].update_one({"_id":ObjectId(event['id'])},{'$unset':{"tbids."+str(event['index']):1}})
        res2 = db['partyPayment'].update_one({"_id":ObjectId(event['id'])},{'$pull':{"tbids":None}})
        res3 = db['TurnBook_2020_2021'].update_one({"_id":ObjectId(event['truckid'])},{'$set':{"paymentid":"617114b7baa1bf3b9386a6a9"}})
        return {'Status': "Delete"}
    elif method == 'addIdsToPartyAndTB':
        res1 = db['partyPayment'].update_one({"_id":ObjectId(event['id'])},{'$addToSet':{'tbids':{'$each':event['tbids']}}})
        res2 = db['TurnBook_2020_2021'].update_many({'_id':{'$in':functions.returnObjectIdArray(event['tbids'])}},{'$set':{"paymentid":event['id']}})
        return {'Status': "Update",'Message':'Added'}
    elif method == 'pendingPayment':
        pipelineTurn=[
    {'$match': {'paymentid': '617114b7baa1bf3b9386a6a9','partyid':event['partyid']}}, 
    {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'}}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
    {'$project': {'data': {'$concat': ['$loadingDate', '_', '$datetruck', '_', {'$convert': {'input': '$_id', 'to': 2, 'onError': '', 'onNull': ''}}]}, 'lrno': 1, 'partyName': {'$arrayElemAt': ['$partyDetails.name', 0]}}}]
        col=db['TurnBook_2020_2021']
        arr = {'Data':[],'Status':'DisplayCommon'}
        res=col.aggregate(pipelineTurn)
        for i in res:
            arr['Data'].append(i)
        return json.loads(json.dumps(arr, default=functions.json_unknown_type_handler))
        
        