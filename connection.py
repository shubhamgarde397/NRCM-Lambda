import pymongo
from pymongo import MongoClient
import consts
import functions

# cluster = MongoClient(consts.mongoConnectionString)
# db = cluster['NRCM_Information']

def cluster(user):
    return MongoClient(consts.mongoConnectionString.replace('NRCM_Information',functions.connectionString(user)))
def db(user):
    # return cluster[functions.connectionString(user)]#Instead of writing cluster i wrote the definition of cluster
    return MongoClient(consts.mongoConnectionString.replace('NRCM_Information',functions.connectionString(user)))[functions.connectionString(user)]
