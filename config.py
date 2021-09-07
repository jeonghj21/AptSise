import os

db = {
    'user'     : 'jhj',
    'password' : '1111',
    'host'     : '127.0.0.1',
    'port'     : '3306',
    'database' : 'prd'
}

DB_URL = f"mysql+mysqlconnector://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?charset=utf8" 

BASE_URL = "http://www.aptsise.kr/"

BASE_DIR = os.environ['HOME'] + "/aptsise"

API_KEY = "kowxleiR8vdBv9Du%2BV5P%2BiRZkzWVDZZi9P3BzCA8etSREsXh991q8cu4AhU1dsAFxe3btGhEA1%2FupLgRLn1iQw%3D%3D"

GIT_TOKEN = "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"
