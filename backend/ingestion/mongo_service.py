import os

from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()


MONGO_URI = os.getenv("MONGO_URI")

MONGO_DB = os.getenv("MONGO_DB")

MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")


client = MongoClient(MONGO_URI)

db = client[MONGO_DB]

collection = db[MONGO_COLLECTION]