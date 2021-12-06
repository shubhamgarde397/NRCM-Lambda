import forwarder
import eventFile#Comment this before deployment

def lambda_handler(event, context):
    event=eventFile.eventName#Comment this before deployment
    return forwarder.data(event)
event=eventFile.eventName#Comment this before deployment
print(lambda_handler(event,''))#Comment this before deployment


def dummy(event):
    return forwarder.data(event)