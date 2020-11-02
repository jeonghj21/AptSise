from flask import Flask, request, render_template, json, jsonify
from sqlalchemy import create_engine, text
from json import JSONEncoder

app = Flask(__name__)
app.config.from_pyfile('config.py')

app.engine = create_engine(app.config['DB_URL'], encoding = 'utf-8')

@app.route("/")
def index():
    return render_template('index.html')
    

@app.route("/getSale")
def getSale():
    params = request.args.to_dict()
    apt = params['apt']

    sql = "select date_format(saled, '%Y%m') ym"
    sql += ", round(sum(price)/(sum(area)/3.3),1) price"
    sql += ", count(id) count"
    sql += " from apt_sale s"
    sql += " where apt_id=" + apt
    sql += " group by date_format(saled, '%Y%m') order by date_format(saled, '%Y%m')"
    print(sql)
    with app.engine.connect() as connection:
        result = connection.execute(text(sql))

    rows=result.fetchall()            
    json_data = { 'labels': [], 'data':[] }
    min_ym = ''
    max_ym = ''
    ym_price = {}
    for r in rows:
        ym = r['ym']
        if min_ym == '': 
            min_ym = ym
        max_ym = ym
        ym_price[ym] = r['price']
    
    while min_ym <= max_ym:
        json_data['labels'].append(min_ym)
        if ym_price.__contains__(min_ym):
            json_data['data'].append(ym_price[min_ym])
        else:
            json_data['data'].append(None)
        yy = int(min_ym[:4])
        mm = int(min_ym[4:6])
        if mm == 12:
            yy = yy + 1
            mm = 1
        else:
            mm = mm + 1
        min_ym = str(yy) + str(mm).zfill(2)

    json_return=json.dumps(json_data)   #string #json
 
    return jsonify(json_return)

@app.route("/getDong")
def getDong():

    data = []
    regions = []

    regions.append(['11680', '강남구'])
    regions.append(['11740', '강동구'])
    regions.append(['11305', '강북구'])
    regions.append(['11500', '강서구'])
    regions.append(['11620', '관악구'])
    regions.append(['11215', '광진구'])
    regions.append(['11530', '구로구'])
    regions.append(['11545', '금천구'])
    regions.append(['11350', '노원구'])
    regions.append(['11320', '도봉구'])
    regions.append(['11230', '동대문구'])
    regions.append(['11590', '동작구'])
    regions.append(['11440', '마포구'])
    regions.append(['11410', '서대문구'])
    regions.append(['11650', '서초구'])
    regions.append(['11200', '성동구'])
    regions.append(['11290', '성북구'])
    regions.append(['11710', '송파구'])
    regions.append(['11470', '양천구'])
    regions.append(['11560', '영등포구'])
    regions.append(['11170', '용산구'])
    regions.append(['11380', '은평구'])
    regions.append(['11110', '종로구'])
    regions.append(['11140', '중구'])
    regions.append(['11260', '중랑구'])

    data.append(regions)

    dongs = {}
    for r in regions:
        dongs_in_r = []
        sql = "select * from apt_dong"
        sql += " where region_cd = '" + r[0] + "' and valid = 'Y'"
        sql += " order by dong_name"
        with app.engine.connect() as connection:
            result = connection.execute(text(sql))
        for res in result:
            dongs_in_r.append([res['dong_cd'], res['dong_name']])
        dongs[r[0]] = dongs_in_r

    data.append(dongs);
    
    json_return=json.dumps(data)   #string #json
 
    return jsonify(json_return)

@app.route("/getApt")
def getApt():

    params = request.args.to_dict()
    region = params['region']
    dong = params['dong']

    sql = "select id, apt_name from apt_master"
    sql += " where dong_region1_cd = '" + region + "'"
    sql += "   and dong_region2_cd = '" + dong + "'"
    sql += " order by apt_name"
    with app.engine.connect() as connection:
        result = connection.execute(text(sql))

    data = []
    for r in result:
        data.append([r['id'], r['apt_name']])
    
    json_return=json.dumps(data)   #string #json
 
    return jsonify(json_return)
