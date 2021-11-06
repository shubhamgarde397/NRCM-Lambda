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
import consts
import re

# cluster = MongoClient(consts.mongoConnectionString)
# db = cluster['NRCM_Information']

def connectionString(user,username):
    if user == 1:#Admin
        if username =='shubham':
            return "NRCM_Information"
        else:
            return "NRCM_Testing"
    elif user == 2:#HR
        return "NRCM_Testing"
    else:#Development
        return "NRCM_Testing"
def returnObjectIdArray(data):
    arr=[]
    for i in data:
        arr.append(ObjectId(i))
    return arr

def constants(event):
    method =event['method'].split(',')[0] if len(event['method'].split(','))>1 else event['method']
    many=event['method'].split('.')[1] if len(event['method'].split('.'))>1 else ''
    insider = event['method'].split(',')[1] if len(event['method'].split(','))>1 else ''
    username = event.get('username') if event.get('username')!=None else ''
    turnbookdate = event.get('turnbookDate') if event.get('turnbookDate')!=None else ''
    partyid = event.get('partyid') if event.get('partyid')!=None else ''
    fromDate = event.get('fromDate') if event.get('fromDate')!=None else ''
    toDate = event.get('toDate') if event.get('toDate')!=None else ''
    balanceHireDisplayType = event.get('type') if event.get('type')!=None else ''
    turnbookUpdateNumber = event.get('part') if event.get('part')!=None else 1
    paymentpipeline=consts.paymentpipeline
    paymentpipeline.insert(0,{'$match': {'$and': [{'date': {'$gte': fromDate, '$lte': toDate}}, {'partyid': partyid}]}})
    tables = [{'name':'gstdetails','sort':['name']}, {'name':'ownerdetails','sort':['truckno']},  {'name':'villagenames','sort':['village_name']}]
    arr = []
    i = 0
    roleI = 0
    user=event['user']
    tempResponse = []
    tableName = event['tablename'] if (method!='display') and (method!='displaynew') else ''

def responser(event,tableName,_id,many=''):
    if tableName == 'villagenames':
        return {'_id': _id.inserted_id,'village_name': event['village_name'],'Status':'VillageInsert'}
    elif tableName == 'ownerdetails':
        return {'_id': _id.inserted_id,'Status':'OwnerInsert'}
    elif tableName == 'gstdetails':
        return {'_id': _id.inserted_id,'name': event['name'],'gst': event['gst'],'dest': event['dest'],'Status':'GSTInsert'}
    elif tableName == 'regularparty':
        return {'_id': _id.inserted_id,'name': event['name'],'Status':'RegularPartyInsert'}
    elif tableName == 'RegularTruck':
        return {'_id': _id.inserted_id,'regulartruck': event['regulartruck'],'Status':'RegularTruckInsert'}
    elif tableName == 'BalanceHire':
        if many=='many':
            return {'Status':'BalanceHireInsert'}
        else:
            return {'_id': _id.inserted_id,'Status':'BalanceHireInsert'}
    elif tableName == 'turnbook':
        return {'_id':_id.inserted_id,'Status':'TBInsert'}
    elif tableName == '':
        return {'_id': '','Status':'InsertDuplicate','StatusToSend':'Duplicate Entry Found.'}
    elif tableName == 'partyPayment':
        return {'Status':'PaymentInsert'}
    elif tableName == 'MailDetails':
        return {'_id':_id.inserted_id,'Status':'MailInsert'}
    elif tableName == 'ignore':
        return {'_id': '','Status':'Ignore','StatusToSend':'Server Error dx0562FrCCx!404O. Unknown Error Occured, Server shutdown'}
        
def insertOrUpdate(tableName,event,method,many='',updatePrint=False):
        turnbookUpdateNumber = event.get('part') if event.get('part')!=None else 1
        if tableName == 'villagenames':
            return {'village_name': event['village_name']}
        elif tableName == 'ownerdetails':
            return {'truckno': event['truckno'],'oname': event['oname'],'pan': event['pan'],'contact': event['contact'],'drivingLic':event['drivingLic'],'regCard':event['regCard'],'accountDetails':event['accountDetails'],'preferences':event['preferences'],'reference':event['reference']}
        elif tableName == 'gstdetails':
            return {'name': event['name'],'gst': event['gst'],'dest': event['dest']}
        elif tableName == 'regularparty':
            return {'name': event['name']}
        elif tableName == 'RegularTruck':
            return {'regulartruck': event['regulartruck']}
        elif tableName == 'BalanceHire':
            if many=='many':
                return event['bhData']
            else:
                return {'truckData':event['truckData'],'todayDate':event['todayDate'],'bankName':event['bankName'],'ifsc':event['ifsc'],'accountNumber':event['accountNumber'],'accountName':event['accountName'],'comments':event['comments'],'print':event['print']}
                # if event['fromtbdisplay']:
                #     return {'truckData':event['bhData'][0]['truckData'],'todayDate':event['todayDate'],'bankName':event['bhData'][0]['bankName'],'ifsc':event['bhData'][0]['ifsc'],'accountNumber':event['bhData'][0]['accountNumber'],'accountName':event['bhData'][0]['accountName'],'comments':event['bhData'][0]['comments'],'print':event['bhData'][0]['print']}
                # else:#These comments are regarding Balance Hire by clicking on buttons from turnbook display
                #     return {'truckData':event['truckData'],'todayDate':event['todayDate'],'bankName':event['bankName'],'ifsc':event['ifsc'],'accountNumber':event['accountNumber'],'accountName':event['accountName'],'comments':event['comments'],'print':event['print']}
        elif tableName == 'partyPayment':
            return event['partyData']
        elif tableName == 'MailDetails':
            return {'partyid': event['partyid'],'loadingFrom': event['loadingFrom'],'loadingTo': event['loadingTo'],'paymentFrom': event['paymentFrom'],'paymentTo': event['paymentTo'],'balanceFollowMsg': event['balanceFollowMsg'],'balanceFollowAmount': event['balanceFollowAmount'],'mailSentDate': event['mailSentDate']}
        elif tableName == 'turnbook':
            if method == 'insert':
                return {"placeid":event["placeid"],"loadingDate":event['loadingDate'],"ownerid":event["ownerid"],"partyid":event["partyid"],"partyType":event["partyType"],"turnbookDate":event["turnbookDate"],"entryDate":event["entryDate"],"datetruck":event['turnbookDate']+'_'+event['truckno'],"lrno":event['lrno'],"advance":event['advance'],"balance":event['balance'],"hamt":event['hamt'],"pochDate":event['pochDate'],"pochPayment":event['pochPayment'],"pgno":event['pgno'],"input":event['input'],"paymentid":event['paymentid']}
            if method == 'update':
                if turnbookUpdateNumber==1:
                    return {"placeid":event["placeid"],"loadingDate":event['loadingDate'],"lrno":event["lrno"],"ownerid":event["ownerid"],"partyid":event["partyid"],"partyType":event["partyType"],"turnbookDate":event["turnbookDate"],"entryDate":event["entryDate"],"hamt":event["hamt"],"advance":event["advance"],"balance":event["balance"],"pochDate":event["pochDate"],"pochPayment":event["pochPayment"],"pgno":event['pgno'],"paymentid":event['paymentid']}
                elif turnbookUpdateNumber==2:
                    return {"placeid":event["placeid"],"lrno":event["lrno"],"partyid":event["partyid"],"hamt":event["hamt"]}
                else:
                    return {"ownerid":event["ownerid"],"datetruck":event['turnbookDate']+'_'+event['truckno']}
        

def createColName(tableName,date1,dummy):
    if tableName == 'turnbook':
        return 'TurnBook_2020_2021'
    else:
        return tableName
        
def considerArrayData(event,db,tables,roleI,role,username):
    arr = []
    pipeline=consts.pipeline
    pipeline=removeMatch(pipeline)
    pipeline.insert(0,{'$match': {'name': username}})
    
    i = 0
    for j in tables:
        obj = {}
        obj[j['name']]=[]
        arr.append(obj)
    for t in tables:
        if t['consider'] == 1:
            for r in db[t['name']].find().sort(t['sort'][0]):
                arr[i][t['name']].append(r)
            i = i + 1
            roleI = i
        else:
            arr[i][t['name']].append({})
            i = i + 1
            roleI = i
    if role == 1:
        col=db['users']
        res=col.aggregate(pipeline)
        data={}
        for i in res:
            data=i;
        data = data if len(data)>0 else {'Role': [{'id': 6.0}]}
        data=data['Role'][0]['id']
        tempObj={'role':int(data)}
        arr.append(tempObj)
    else:
        data = {'Role': [{'id': 6.0}]}
        data=data['Role'][0]['id']
        tempObj={'role':int(data)}
        arr.append(tempObj)
    arr.append({'Status':'Display'})
    return arr
    
def getPaymentPipeline(event):
    type=event['display']
    if type == 1:
        paymentpipeline=consts.paymentpipeline2
        paymentpipeline=removeMatch(paymentpipeline)
        paymentpipeline.insert(0,{'$match': {'partyid':{'$in':event['partyid']},'done':False}})
        return paymentpipeline
    elif type == 2:
        paymentpipeline=consts.paymentpipeline2
        paymentpipeline=removeMatch(paymentpipeline)
        paymentpipeline.insert(0,{'$match':{'$and':[{'date':{'$gte':event['from']}},{'date':{'$lte':event['to']}},{'done':False}]}})
        return paymentpipeline
    elif type == 3:
        paymentpipeline=consts.paymentpipeline2
        paymentpipeline=removeMatch(paymentpipeline)
        paymentpipeline.insert(0,{'$match':{'$and':[{'date':{'$gte':event['from']}},{'date':{'$lte':event['to']}},{'partyid':{'$in':event['partyid']}},{'done':False}]}})
        return paymentpipeline
    
def removeMatch(pipeline):
    try:
        a=pipeline[0]['$match']
        pipeline.pop(0)
    except KeyError:
        pass
    except IndexError:
        pass
    return pipeline
    
def twoDigit(d):
	if len(str(d))==1:
		return '0'+str(d)
	else:
		return str(d)

def convertToDict(lst):
    f=list(map(lambda x: x['_id'][-2:],lst))
    res_dct = {f[i]:True for i in range(len(f))}
    return res_dct

def completeTheMonth(arr):
    tp=[]
    j=0
    y = str(arr[0]['_id'][:4])
    m = str(arr[0]['_id'][5:7])
    d = convertToDict(arr)
    for i in range(consts.months[arr[0]['_id'][5:7]]):
        try:
            e = d[twoDigit(i+1)]
        except KeyError:
            e = False
        if e:
            if arr[j]['print']==True:
                arr[j]['color']='#5CB85C'
            else:
                arr[j]['color']='#D9534F'
            tp.append(arr[j])
            j = j + 1
        else:
            temp = {}
            temp['_id']= y+'-'+m+'-'+twoDigit(i+1)
            temp['data']='NA'
            temp['print'] = False
            temp['color']='#686868'
            tp.append(temp)
    return tp
    
def addMonthYear(data,a):
    a[0]['$match']['$and'][1]['loadingDate'] = re.compile(data)
    return a
    
def json_unknown_type_handler(x):
    if isinstance(x, bson.ObjectId):
        return str(x)
    raise TypeError("Unknown datetime type")
    