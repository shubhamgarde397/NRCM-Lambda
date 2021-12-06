def dynamicC(event,type):
    if type=='method':
        return event['method'].split('.')[0] if len(event['method'].split('.'))>1 else event['method']
    elif type=='many':
        return event['method'].split('.')[1] if len(event['method'].split('.'))>1 else ''
    elif type=='insider':
        return event['method'].split('.')[1] if len(event['method'].split('.'))>1 else ''
    elif type=='username':
      return event.get('username') if event.get('username')!=None else ''
    elif type=='turnbookdate':
        return event.get('turnbookDate') if event.get('turnbookDate')!=None else ''
    elif type=='partyid':
        return event.get('partyid') if event.get('partyid')!=None else ''
    elif type=='fromDate':
        return event.get('fromDate') if event.get('fromDate')!=None else ''
    elif type=='toDate':
        return event.get('toDate') if event.get('toDate')!=None else ''
    elif type=='balanceHireDisplayType':
        return event.get('type') if event.get('type')!=None else ''
    elif type=='turnbookUpdateNumber':
        return event.get('part') if event.get('part')!=None else 1
    elif type == 'addtotbids':
        return event.get('addtotbids') if event.get('addtotbids')!=None else False
    elif type=='lrnoExists':
      return True if event.get('lrno')!=None else False
    elif type == 'getlrno':
         return event.get('lrno') if event.get('lrno')!=None else ''
    elif type == 'HiddenEntry':
         return event.get('HiddenEntry') if event.get('HiddenEntry')!=None else False
    elif type == 'showUnhideTruckFromTB':
         return True if event.get('updateTruck')!=None else False
