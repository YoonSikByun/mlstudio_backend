import os
import json

def readJSONFile(file_path : str) :
    jsonData = None
    if os.path.exists(file_path):
        with open(file_path, 'r') as fp :
            jsonData = json.load(fp)

    return jsonData
    # return json.dumps(jsonData, indent=4)

def readTextFile(file_path : str) :
    textData = ''
    if os.path.exists(file_path):
        with open(file_path, 'r') as fp :
            textData = fp.read()

    return {'text' : textData}