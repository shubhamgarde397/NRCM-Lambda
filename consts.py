# PIPELINES
pipeline=[
    {'$addFields': {'newroleid': {'$toObjectId': '$roleid'}}}, 
    {'$lookup': {'from': 'roles', 'localField': 'newroleid', 'foreignField': '_id', 'as': 'Role'}},
    {'$project': {'roleid': 0, 'newroleid': 0}}
]

paymentpipeline=[
    {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'}}},
    {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}},
    {'$project': {'partyid': 0, 'newpartyid': 0}}
    ]
paymentpipeline2 = [
{'$addFields': {'newpartyid': {'$toObjectId': '$partyid'}}}, 
{'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyArray'}}, 
{'$addFields': {'partyName': {'$first': '$partyArray.name'}}}, 
{'$project': {'partyid': 0, 'newpartyid': 0, 'partyArray': 0}},
{'$sort':{'date':1}}
]
    
pipelineTurn=[
                {'$addFields': {'newplaceid': {'$toObjectId': '$placeid'}, 'newpartyid': {'$toObjectId': '$partyid'}, 'newownerid': {'$toObjectId': '$ownerid'},'checker':0}},
                {'$lookup': {'from': 'villagenames', 'localField': 'newplaceid', 'foreignField': '_id', 'as': 'villageDetails'}},
                {'$lookup': {'from': 'ownerdetails', 'localField': 'newownerid', 'foreignField': '_id', 'as': 'ownerDetails'}},
                {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
                {'$project': {'placeid': 0, 'ownerid': 0, 'partyid': 0, 'newplaceid': 0, 'newownerid': 0, 'newpartyid': 0, 'datetruck': 0}},
                {'$sort': {'loadingDate': 1}}
            ]
pipelineTurnNew=[{'$match': {
                '$and': [{'partyType': 'NRCM'}, {'loadingDate': 'changeThis'}, 
                {'$or': [{'lrno': 0}, {'hamt': ''}, {'placeid': '5bcdecdab6b821389c8abde0'},{'partyid': '5fff37a31f4443d6ec77e078'}]}]}},
                {'$sort': {'loadingDate': 1}},
                {'$addFields': {'truckno': {'$arrayElemAt': [{'$split': ['$datetruck', '_']}, 1]}}}]

pipelinePaymentPP1=[
        {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'},'type':'payment'}}, 
        {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
        {'$project': {'partyName': {'$first': '$partyDetails.name'}, 'amount': 1, 'date': 1,'_id':0,'type':1}}]

pipelinePaymentPP2=[
    {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'},'type':'buy', 'newownerid': {'$toObjectId': '$ownerid'}}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'newownerid', 'foreignField': '_id', 'as': 'truckDetails'}},
    {'$project': {'partyName': {'$first': '$partyDetails.name'},'truckNo': {'$first': '$truckDetails.truckno'}, 'date': '$loadingDate', 'amount': '$hamt', 'lrno': 1, '_id': 0,'type':1}}]
    
    
# PIPELINES
# CONSTANTS
mongoConnectionString='mongodb+srv://Shubham:leogarde1!@24728293031365366.zyejm.mongodb.net/NRCM_Information?retryWrites=true&w=majority'
tables = [{'name':'gstdetails','sort':['name']}, {'name':'ownerdetails','sort':['truckno']},  {'name':'villagenames','sort':['village_name']}]
tables2 = [{'name':'gstdetails','sort':['name']}, {'name':'villagenames','sort':['village_name']}]
owner =  {'name':'ownerdetails','sort':['truckno']}
months={"01":31,"02":29,"03":31,"04":30,"05":31,"06":30,"07":31,"08":31,"09":30,"10":31,"11":30,"12":31}
arr = []
i = 0
roleI = 0
# CONSTANTS
