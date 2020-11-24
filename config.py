db = {
    'user'     : 'jhj',
    'password' : '1111',
    'host'     : '127.0.0.1',
    'port'     : '3306',
    'database' : 'prj'
}

DB_URL = f"mysql+mysqlconnector://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?charset=utf8" 

BASE_URL = "http://localhost:5000/"
