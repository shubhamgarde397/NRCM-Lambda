import forwarder
import eventFile

def lambda_handler(event, context):
    event=eventFile.eventName#Comment this before deployment
    return forwarder.data(event)
event=eventFile.eventName#Comment this before deployment
print(lambda_handler(event,''))#Comment this before deployment