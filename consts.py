import dynamicConstants
# PIPELINES
pipeline=[
    {'$addFields': {'newroleid': {'$toObjectId': '$roleid'}}}, 
    {'$lookup': {'from': 'roles', 'localField': 'newroleid', 'foreignField': '_id', 'as': 'Role'}},
    {'$project': {'roleid': 0, 'newroleid': 0}}
]

singleTruckPipeline=[
    {'$addFields': {'o': {'$toObjectId': '$ownerid'}, 'p': {'$toObjectId': '$partyid'}, 'v': {'$toObjectId': '$placeid'}}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'o', 'foreignField': '_id', 'as': 'trucks'}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'p', 'foreignField': '_id', 'as': 'parties'}}, 
    {'$lookup': {'from': 'villagenames', 'localField': 'v', 'foreignField': '_id', 'as': 'places'}}, 
    {'$project': {'partyType': 1, 'turnbookDate': 1, 'loadingDate': 1, 'lrno': 1, 'hamt': 1, 'pochDate': 1,
        'pgno': 1, 'truckName': {'$arrayElemAt': ['$trucks', 0]}, 'placeName': {'$arrayElemAt': ['$places', 0]}, 'partyName': {'$arrayElemAt': ['$parties', 0]},
        'advance': 1, 'balance': 1, 'pochPayment': 1, 'invoice': '', 'complete': 1, 'typeOfLoad': 1, 'completeDate': 1}}]

paymentpipeline=[
    {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'}}},
    {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}},
    {'$project': {'partyid': 0, 'newpartyid': 0}}
    ]
paymentpipeline2 =[ 
    {'$unwind': {'path': '$tbids'}}, 
    {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'}, 'newtbid': {'$toObjectId': '$tbids'}}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyArray'}}, 
    {'$lookup': {'from': 'TurnBook_2020_2021', 'localField': 'newtbid', 'foreignField': '_id', 'as': 'turnbook'}}, 
    {'$addFields': {'partyName': {'$first': '$partyArray.name'},'partyid': {'$convert': {'input': {'$first': '$partyArray._id'},'to': 2,'onError': '','onNull': ''}}, 'loadingDate': {'$concat': [{'$convert': {'input': {'$first': '$turnbook._id'}, 'to': 2, 'onError': '', 'onNull': ''}}, '_',{'$first': '$turnbook.loadingDate'}, '_', {'$first': '$turnbook.datetruck'}]}}}, 
    {'$project': {'newpartyid': 0, 'partyArray': 0, 'newtbid': 0, 'turnbook': 0}}, 
    {'$group': {'_id': '$_id', 'partyName': {'$push': '$partyName'},'partyid': {'$push': '$partyid'}, 'date': {'$push': '$date'}, 'amount': {'$push': '$amount'}, 'loadingDate': {'$push': '$loadingDate'}}}, 
    {'$project': {'_id': {'$toString': '$_id'}, 'partyName': {'$arrayElemAt': ['$partyName', 0]},'partyid': {'$arrayElemAt': ['$partyid', 0]}, 'date': {'$arrayElemAt': ['$date', 0]}, 'amount': {'$arrayElemAt': ['$amount', 0]}, 'loadingDate':{'$slice':['$loadingDate',1,10]}}},
    {'$sort':{'date':1}}
    ]
# [
# {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'}}}, 
# {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyArray'}}, 
# {'$addFields': {'partyName': {'$first': '$partyArray.name'}}}, 
# {'$project': {'partyid': 0, 'newpartyid': 0, 'partyArray': 0}},
# {'$sort':{'date':1}}
# ]
    
pipelineTurn=[
                {'$addFields': {'newplaceid': {'$toObjectId': '$placeid'}, 'newpartyid': {'$toObjectId': '$partyid'}, 'newownerid': {'$toObjectId': '$ownerid'}, 'newpaymentid': {'$toObjectId': '$paymentid'},'checker':0}},
                {'$lookup': {'from': 'villagenames', 'localField': 'newplaceid', 'foreignField': '_id', 'as': 'villageDetails'}},
                {'$lookup': {'from': 'ownerdetails', 'localField': 'newownerid', 'foreignField': '_id', 'as': 'ownerDetails'}},
                {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
                {'$lookup': {'from': 'partyPayment', 'localField': 'newpaymentid', 'foreignField': '_id', 'as': 'paymentDetails'}}, 
                {'$project': {'placeid': 0, 'ownerid': 0, 'partyid': 0, 'newplaceid': 0, 'newownerid': 0, 'newpartyid': 0, 'datetruck': 0,'paymentid':0,'newpaymentid':0}},
                {'$sort': {'loadingDate': 1}}
            ]
pipelineTurnS14=[
    {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'}, 'newownerid': {'$toObjectId': '$ownerid'}}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'newownerid', 'foreignField': '_id', 'as': 'ownerDetails'}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
    {'$project': {'truckno': {'$arrayElemAt': ['$ownerDetails.truckno', 0]}, 'partyName': {'$arrayElemAt': ['$partyDetails.name', 0]}, 'loadingDate': 1, 'turnbookDate': 1, 'truckid': {'$toString': {'$arrayElemAt': ['$ownerDetails._id', 0]}}}}, 
    {'$sort': {'loadingDate': 1}}]
pipelineTurnLocation=[
    {'$addFields': {'lkl': {'$last': '$locations'}}}, 
    {'$match': 
      {
        #   '$expr': {'$ne': ['$placeid', '$lkl']},
          'loadingDate': {'$nin': ['']},
          'complete':False
      }
  }, 
  {'$unwind': {'path': '$locations'}}, 
  {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'},'newplaceid': {'$toObjectId': '$placeid'}, 'newownerid': {'$toObjectId': '$ownerid'}, 'newLocationid': {'$toObjectId': '$locations'}}}, 
  {'$lookup': {'from': 'villagenames', 'localField': 'newplaceid', 'foreignField': '_id', 'as': 'villageArray'}}, 
  {'$lookup': {'from': 'villagenames', 'localField': 'newLocationid', 'foreignField': '_id', 'as': 'locationsArray'}}, 
  {'$lookup': {'from': 'ownerdetails', 'localField': 'newownerid', 'foreignField': '_id', 'as': 'ownerDetails'}}, 
  {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
  {'$group': {'_id': '$_id','oD':{'$push': {'$arrayElemAt': ['$ownerDetails', 0]}}, 'destination':{'$push': {'$arrayElemAt': ['$villageArray.village_name', 0]}},'loadingDate': {'$push': '$loadingDate'}, 'partyType': {'$push': '$partyType'}, 'turnbookDate': {'$push': '$turnbookDate'},'locationDate': {'$push': '$locationDate'}, 'truckno': {'$push': {'$arrayElemAt': ['$ownerDetails.truckno', 0]}}, 'partyName': {'$push': {'$arrayElemAt': ['$partyDetails.name', 0]}}, 'contact': {'$push': {'$arrayElemAt': ['$ownerDetails.contact', 0]}}, 'locationsArray': {'$push': {'$arrayElemAt': ['$locationsArray.village_name', 0]}},'lkl':{'$push': '$lkl'},'placeid':{'$push':'$placeid'}}}, 
  {'$addFields':{
            'pE': {'$cond': {'if': {'$lte': [{'$first':'$oD.policyExpiry'}, '2021-11-26']}, 'then': '#D9534F', 
            'else': {'$cond': {'if': {'$and': [{'$gte': [{'$first':'$oD.policyExpiry'}, '2021-11-26']}, {'$lte': [{'$first':'$oD.policyExpiry'}, '2022-02-26']}]}, 'then': '#FFA500', 'else': '#5CB85C'}}}}, 
            'dE': {'$cond': {'if': {'$lte': [{'$first':'$oD.drivingLicExpiry'}, '2021-11-26']}, 'then': '#D9534F', 
            'else': {'$cond': {'if': {'$and': [{'$gte': [{'$first':'$oD.drivingLicExpiry'}, '2021-11-26']}, {'$lte': [{'$first':'$oD.drivingLicExpiry'}, '2022-02-26']}]}, 'then': '#FFA500', 'else': '#5CB85C'}}}}, 
            'rE': {'$cond': {'if': {'$lte': [{'$first':'$oD.regCardExpiry'}, '2021-11-26']}, 'then': '#D9534F', 
            'else': {'$cond': {'if': {'$and': [{'$gte': [{'$first':'$oD.regCardExpiry'}, '2021-11-26']}, {'$lte': [{'$first':'$oD.regCardExpiry'}, '2022-02-26']}]}, 'then': '#FFA500', 'else': '#5CB85C'}}}}, 
            'fE': {'$cond': {'if': {'$lte': [{'$first':'$oD.fitnessExpiry'}, '2021-11-26']}, 'then': '#D9534F', 
            'else': {'$cond': {'if': {'$and': [{'$gte': [{'$first':'$oD.fitnessExpiry'}, '2021-11-26']}, {'$lte': [{'$first':'$oD.fitnessExpiry'}, '2022-02-26']}]}, 'then': '#FFA500', 'else': '#5CB85C'}}}},
            'typeOfVehiclefirst':{'$substr':[{'$first':'$oD.typeOfVehicle'},0,1]},
            'P':{'$cond':{'if':{'$ne':[{'$first':'$oD.pan'},'']},'then':'#5CB85C','else':'#D9534F'}},
            'A':{'$cond':{'if':{'$eq':[{'$size':'$oD.accountDetails'},0]},'then':'#D9534F','else':'#5CB85C'}},
            'updateTBL': {'$cond': {'if':{'$eq':[{'$first':'$placeid'}, {'$first':'$lkl'}]},'then':True,'else':False}}}},
  {'$project': {'updateTBL':1,'pE':1,'dE':1,'rE':1,'fE':1,'P':1,'A':1, 'oD': {'$first':'$oD'},'truckno': {'$first': '$truckno'},'destination': {'$first': '$destination'}, 'partyName': {'$first': '$partyName'}, 'loadingDate': {'$first': '$loadingDate'}, 'turnbookDate': {'$first': '$turnbookDate'}, 'contact': {'$first': '$contact'}, 'partyType': {'$first': '$partyType'}, 'locationsArray': 1, 'locationDate': {'$first': '$locationDate'}}},
  {'$sort': {'loadingDate': 1}}
    #update project pipeline, add more lookups, destinations, locations upto now, location dates.
    # delete and update operations to location and location date, location date must be set to today date and add yesterday tomorrow buttons.
    ]
pipelineTurnNew=[{'$match': {
                '$and': [{'partyType': 'NRCM'}, {'loadingDate': 'changeThis'}, 
                {'$or': [{'lrno': 0}, {'hamt': ''}, {'placeid': '5bcdecdab6b821389c8abde0'},{'partyid': '5fff37a31f4443d6ec77e078'}]}]}},
                {'$sort': {'loadingDate': 1}},
                {'$addFields': {'truckno': {'$arrayElemAt': [{'$split': ['$datetruck', '_']}, 1]}}}]

pipelinePaymentPP1=[
        {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'},'type':'payment'}}, 
        {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
        {'$project': {'partyName': {'$first': '$partyDetails.name'}, 'amount': 1, 'date': 1,'_id':0,'type':1}},
        {'$sort': {'date': 1}}]

pipelinePaymentPP2=[
    {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'},'type':'buy', 'newownerid': {'$toObjectId': '$ownerid'},'newplaceid': {'$toObjectId': '$placeid'}}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'newownerid', 'foreignField': '_id', 'as': 'truckDetails'}},
    {'$lookup': {'from': 'villagenames', 'localField': 'newplaceid', 'foreignField': '_id', 'as': 'villageDetails'}},
    {'$project': {'partyName': {'$first': '$partyDetails.name'},'truckNo': {'$first': '$truckDetails.truckno'},'placeName': {'$first': '$villageDetails.village_name'}, 'date': '$loadingDate', 'amount': '$hamt', 'lrno': 1, '_id': 0,'type':1}},
    {'$sort': {'loadingdate': 1}}]
    
pipelineMailDisplay=[
    {'$addFields': {'p': {'$toObjectId': '$partyid'}}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'p', 'foreignField': '_id', 'as': 'partyDetails'}}, 
    {'$project': {'p': 0}}]
    
    
#Chartpipeline
pipelineChartByYear=[
    {'$group': {'_id': '$loadingDate', 'sum': {'$sum': 1}}}, 
    {'$sort': {'_id': 1}}]
pipelineChartByPartyYearWise=[
    {'$project': {'month': {'$toInt': {'$arrayElemAt': [{'$split': ['$loadingDate', '-']}, 1]}}}}, 
    {'$group': {'_id': '$month', 'sum': {'$sum': 1}}}, 
    {'$sort': {'_id': 1}}, 
    {'$addFields': {'_id': {'$let': {'vars': {'monthsinString': ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']}, 'in': {'$arrayElemAt': ['$$monthsinString', '$_id']}}}}}]
    
pipelineChartBySelectedPartyYearwise=[

    {'$project': {'month': {'$toInt': {'$arrayElemAt': [{'$split': ['$loadingDate', '-']}, 1]}}, 'partyid': 1}}, 
    {'$group': {'_id': '$partyid', 'sum': {'$sum': 1}}}, 
    {'$addFields': {'_id': {'$toObjectId': '$_id'}}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': '_id', 'foreignField': '_id', 'as': 'partyDetails'}}, 
    {'$addFields': {'_id': {'$first': '$partyDetails.name'}}},
    {'$project': {'_id': 1, 'sum': 1}}
    ]
#Chartpipeline
    # REPORT
pipelineReport=[
    {'$addFields': {'newplaceid': {'$toObjectId': '$placeid'}, 'newpartyid': {'$toObjectId': '$partyid'}, 'newownerid': {'$toObjectId': '$ownerid'}}}, 
    {'$lookup': {'from': 'villagenames', 'localField': 'newplaceid', 'foreignField': '_id', 'as': 'villageDetails'}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'newownerid', 'foreignField': '_id', 'as': 'ownerDetails'}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
    {'$project': {'V': {'$arrayElemAt': ['$villageDetails', 0]}, 'P': {'$arrayElemAt': ['$partyDetails', 0]}, 'T': {'$arrayElemAt': ['$ownerDetails', 0]}, 'loadingDate': 1}}, 
    {'$project': {'village_name': '$V.village_name', 'party_name': '$P.name', 'truck_no': '$T.truckno', 'contact': '$T.contact', 'loadingDate': 1}},
    # {'$match':{'party_name':{'$nin':["NRCM"]}}},
    {'$match':{'loadingDate':{'$ne':""}}},
    {'$sort':{'loadingDate':1}}
    ]
pipelinePanAll=[
    {'$addFields': {'owner': {'$toObjectId': '$ownerid'}}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'owner', 'foreignField': '_id', 'as': 'trucks'}},
    {'$project': {'trucks': 1, 'truck': {'$arrayElemAt': ['$trucks', 0]}, 'loadingDate': 1, 'partyType': 1}}, 
    {'$project': {'pan': '$truck.pan', 'truckno': '$truck.truckno', 'oname': '$truck.oname', 'loadingDate': 1, 'partyType': 1}}, 
    {'$match': {'$and': [{'pan': {'$eq': ''}}, {'oname': {'$eq': ''}}]}}, 
    {'$project': {'truckno': 1, 'loadingDate': 1, 'partyType': 1}}, 
    {'$group': {'_id': '$partyType', 'sum': {'$sum': 1}}}
    ]
pipelinePanDateWise=[
    {'$addFields': {'owner': {'$toObjectId': '$ownerid'}}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'owner', 'foreignField': '_id', 'as': 'trucks'}},
    {'$project': {'trucks': 1, 'truck': {'$arrayElemAt': ['$trucks', 0]}, 'loadingDate': 1, 'partyType': 1}}, 
    {'$project': {'pan': '$truck.pan', 'truckno': '$truck.truckno', 'oname': '$truck.oname', 'loadingDate': 1, 'partyType': 1}}, 
    {'$match': {'$and': [{'pan': {'$eq': ''}}, {'oname': {'$eq': ''}}]}}, 
    {'$project': {'truckno': 1, 'loadingDate': 1, 'partyType': 1}}, 
    {'$group': {'_id': '$partyType', 'sum': {'$sum': 1}}}
    ]
pipelinePanTruckData=[
    {'$addFields': {'owner': {'$toObjectId': '$ownerid'}}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'owner', 'foreignField': '_id', 'as': 'trucks'}},
    {'$project': {'trucks': 1, 'truck': {'$arrayElemAt': ['$trucks', 0]}, 'loadingDate': 1, 'partyType': 1}}, 
    {'$project': {'pan': '$truck.pan', 'truckno': '$truck.truckno', 'oname': '$truck.oname', 'loadingDate': 1, 'partyType': 1}}, 
    {'$match': {'$and': [{'pan': {'$eq': ''}}, {'oname': {'$eq': ''}}]}}, 
    {'$project': {'truckno': 1, 'loadingDate': 1, 'partyType': 1}}, 
    ]
pipelineAccount=[
    {'$addFields': {'owner': {'$toObjectId': '$ownerid'}}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'owner', 'foreignField': '_id', 'as': 'trucks'}}, 
    {'$project': {'trucks': 1, 'truck': {'$arrayElemAt': ['$trucks', 0]}, 'loadingDate': 1}}, 
    {'$project': {'truckno': '$truck.truckno', 'loadingDate': 1, 'accountDetails': '$truck.accountDetails', 'contactDetails': '$truck.contact'}}, 
    {'$match': {'$or': [{'accountDetails': {'$size': 0}}]}},#, {'contactDetails': {'$size': 0}}
    {'$project': {'truckno': 1, 'loadingDate': 1,  'accountDetails':1,'contactDetails':1}}, 
    {'$sort': {'loadingDate': 1}}]
pipelineLastUnloaded=[{'$sort': {'loadingDate': 1}}]
pipelineAccountByDY=[
    {'$addFields': {'owner': {'$toObjectId': '$ownerid'}}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'owner', 'foreignField': '_id', 'as': 'trucks'}}, 
    {'$project': {'trucks': 1, 'truck': {'$arrayElemAt': ['$trucks', 0]}, 'loadingDate': 1}}, 
    {'$project': {'truckno': '$truck.truckno', 'loadingDate': 1, 'accountDetails': '$truck.accountDetails', 'contactDetails': '$truck.contact'}}, 
    {'$match': {'$or': [{'accountDetails': {'$size': 0}}]}},#{'contactDetails': {'$size': 0}} 
    {'$project': {'truckno': 1,'DY': {'$concat': [{'$arrayElemAt': [{'$split': ['$loadingDate', '-']}, 1]}, '-', {'$arrayElemAt': [{'$split': ['$loadingDate', '-']}, 0]}]}}}, 
    {'$group': {'_id': '$DY', 'sum': {'$sum': 1}}}, 
    {'$sort': {'_id': 1}}]
    # REPORT
    
#KUNDLI
pipelineDeep1=[
    {'$addFields': {'ownerid': {'$toObjectId': '$ownerid'}, 'partyid': {'$toObjectId': '$partyid'}, 'placeid': {'$toObjectId': '$placeid'}}}, 
    {'$lookup': {'from': 'ownerdetails', 'localField': 'ownerid', 'foreignField': '_id', 'as': 'trucks'}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'partyid', 'foreignField': '_id', 'as': 'parties'}}, 
    {'$lookup': {'from': 'villagenames', 'localField': 'placeid', 'foreignField': '_id', 'as': 'villages'}}, 
    {'$project': {'_id': 0, 'placeid': 0, 'partyid': 0, 'ownerid': 0, 'input': 0, 'frompartyid': 0}}]
pipelineDeep2=[{'$group': {'_id': '$partyType', 'sum': {'$sum': 1}}}]
pipelineDeep3=[
    {'$project': {'month': {'$toInt': {'$arrayElemAt': [{'$split': ['$loadingDate', '-']}, 1]}}}}, 
    {'$group': {'_id': '$month', 'sum': {'$sum': 1}}}, 
    {'$sort': {'_id': 1}}]
pipelineDeep4=[
    {'$addFields': {'p': {'$toObjectId': '$partyid'}}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'p', 'foreignField': '_id', 'as': 'parties'}}, 
    {'$project': {'n': {'$arrayElemAt': ['$parties', 0]}}}, 
    {'$project': {'na': '$n.name'}}, 
    {'$group': {'_id': '$na', 'sum': {'$sum': 1}}}]
pipelineDeep5=[
    {'$addFields': {'p': {'$toObjectId': '$placeid'}}}, 
    {'$lookup': {'from': 'villagenames', 'localField': 'p', 'foreignField': '_id', 'as': 'villages'}}, 
    {'$project': {'n': {'$arrayElemAt': ['$villages', 0]}}}, 
    {'$project': {'na': '$n.village_name'}}, 
    {'$group': {'_id': '$na', 'sum': {'$sum': 1}}}]
pendingPayment=[
    {'$match': {'paymentid': '617114b7baa1bf3b9386a6a9'}}, 
    {'$addFields': {'newpartyid': {'$toObjectId': '$partyid'}}}, 
    {'$lookup': {'from': 'gstdetails', 'localField': 'newpartyid', 'foreignField': '_id', 'as': 'partyDetails'}}, 
    {'$project': {'data': {'$concat': ['$loadingDate', '_', '$datetruck', '_', {'$convert': {'input': '$_id', 'to': 2, 'onError': '', 'onNull': ''}}]}, '_id': 0, 'lrno': 1, 'partyName': {'$arrayElemAt': ['$partyDetails.name', 0]}}}]
#KUNDLI
# PIPELINES
# CONSTANTS
mongoConnectionString='mongodb+srv://Shubham:leogarde1!@24728293031365366.zyejm.mongodb.net/NRCM_Information?retryWrites=true&w=majority'
tables = [{'name':'gstdetails','sort':['name']}, {'name':'ownerdetails','sort':['truckno']},  {'name':'villagenames','sort':['village_name']},{'name':'missingLRReason','sort':['reason']}]
tablesNew = [
    {'name':'gstdetails','aggregate':[[{'$sort':{'name':1}}]],'jsonName':'gstdetails'}, 
    {'name':'ownerdetails','aggregate':[[{'$match': {'show': True,'empty': True}}, 
    {'$addFields': {
            'pE': {'$cond': {'if': {'$lte': ['$policyExpiry', '2021-11-26']}, 'then': '#D9534F', 
            'else': {'$cond': {'if': {'$and': [{'$gte': ['$policyExpiry', '2021-11-26']}, {'$lte': ['$policyExpiry', '2022-02-26']}]}, 'then': '#FFA500', 'else': '#5CB85C'}}}}, 
            'dE': {'$cond': {'if': {'$lte': ['$drivingLicExpiry', '2021-11-26']}, 'then': '#D9534F', 
            'else': {'$cond': {'if': {'$and': [{'$gte': ['$drivingLicExpiry', '2021-11-26']}, {'$lte': ['$drivingLicExpiry', '2022-02-26']}]}, 'then': '#FFA500', 'else': '#5CB85C'}}}}, 
            'rE': {'$cond': {'if': {'$lte': ['$regCardExpiry', '2021-11-26']}, 'then': '#D9534F', 
            'else': {'$cond': {'if': {'$and': [{'$gte': ['$regCardExpiry', '2021-11-26']}, {'$lte': ['$regCardExpiry', '2022-02-26']}]}, 'then': '#FFA500', 'else': '#5CB85C'}}}}, 
            'fE': {'$cond': {'if': {'$lte': ['$fitnessExpiry', '2021-11-26']}, 'then': '#D9534F', 
            'else': {'$cond': {'if': {'$and': [{'$gte': ['$fitnessExpiry', '2021-11-26']}, {'$lte': ['$fitnessExpiry', '2022-02-26']}]}, 'then': '#FFA500', 'else': '#5CB85C'}}}},
            'typeOfVehiclefirst':{'$substr':['$typeOfVehicle',0,1]}}}, 
            {'$sort': {'truckno': 1}}]
        ],'jsonName':'ownerdetails'},  
    {'name':'villagenames','aggregate':[[{'$sort':{'village_name':1}}]],'jsonName':'villagenames'},
    {'name':'missingLRReason','aggregate':[[{'$sort':{'reason':1}}]],'jsonName':'missingLRReason'},
    {'name':'ownerdetails','aggregate':[[{'$match': {'show': False}}, {'$sort': {'truckno': 1}}]],'jsonName':'hiddenownerdetails'}]
tables2 = [{'name':'gstdetails','sort':['name']}, {'name':'villagenames','sort':['village_name']}]
owner =  {'name':'ownerdetails','sort':['truckno']}
months={"01":31,"02":29,"03":31,"04":30,"05":31,"06":30,"07":31,"08":31,"09":30,"10":31,"11":30,"12":31}
arr = []
i = 0
roleI = 0
# CONSTANTS
