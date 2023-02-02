import firebase_admin
import os
import json
from firebase_admin import credentials, firestore

'''
    json ファイルのデータを firestore にアップロードする
'''

cred = credentials.Certificate(
    "UploadJsonToFirestore/tangotchi-exp-firebase-adminsdk-l8ckn-26419a10ba.json")
app = firebase_admin.initialize_app(cred)

db = firestore.client()

filename = "wrongTranslationCandidates.json"
path = os.path.expanduser('~') + "/Downloads/" + filename

if filename.endswith('.json'):
    # filename minus ext will be used as collection name
    collectionName = filename.split('.')[0]
    f = open(path, 'r')
    docs = json.loads(f.read())
    # flag = False
    for doc in docs:
        id = doc.get('wordName')
        # if flag:
        if id:
            db.collection("WrongTranslationCandidates").document(
                id).set(doc, merge=True)
        else:
            db.collection("WrongTranslationCandidates").add(doc)
        # if id == "true":
        #     flag = True
