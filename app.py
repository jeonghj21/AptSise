from flask import Flask, request, render_template, json, jsonify
from sqlalchemy import create_engine, text
from json import JSONEncoder
from datetime import date

app = Flask(__name__)
app.config.from_pyfile('config.py')

app.engine = create_engine(app.config['DB_URL'], encoding = 'utf-8')

@app.route("/")
def index():
    return render_template('index.html')
    
def spreadDataForYM(rows, ym_label, data_label, data, labels):
    min_ym = ''
    max_ym = ''
    ym_data = {}
    for r in rows:
        ym = r[ym_label]
        if min_ym == '': 
            min_ym = ym
        max_ym = ym
        ym_data[ym] = r[data_label]
    
    while min_ym <= max_ym:
        labels.append(min_ym)
        if ym_data.__contains__(min_ym):
            data.append(ym_data[min_ym])
        else:
            data.append(None)
        yy = int(min_ym[:4])
        mm = int(min_ym[4:6])
        if mm == 12:
            yy = yy + 1
            mm = 1
        else:
            mm = mm + 1
        min_ym = str(yy) + str(mm).zfill(2)

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
    json_data = { 'labels': [], 'data':[], 'rawData':{} }

    spreadDataForYM(rows, 'ym', 'price', json_data['data'], json_data['labels'])

    sql = "select date_format(saled, '%Y%m') ym, price, area, floor"
    sql += " from apt_sale s"
    sql += " where apt_id=" + apt
    sql += " order by saled"
    with app.engine.connect() as connection:
        result = connection.execute(text(sql))

    rows=result.fetchall()            
    ym_raws = {}
    ym_raw = []
    for r in rows:
        if ym_raws.__contains__(r['ym']):
            ym_raw = ym_raws[r['ym']]
        ym_raw.append({ 'area': r['area'], 'floor': r['floor'], 'price': r['price'] })
        ym_raws[r['ym']] = ym_raw

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

    print(len(regions))
    data.append(dongs)
    
    json_return=json.dumps(data)   #string #json
 
    return jsonify(json_return)

@app.route("/getApt")
def getApt():

    params = request.args.to_dict()
    region = params['region']
    dong = params['dong']

    sql = "select id, apt_name"
    sql += "  from apt_master"
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

@app.route("/getSaleStat")
def getSaleStat():
    params = request.args.to_dict()
    from_ym = params['from_ym']
    to_ym = params['to_ym']
    region = params['region']
    dong = params['dong']
    area_type = params['area_type']
    ages = params['ages']
    age_sign = params['age_sign']

    sql = " select ym, cast(round(avg(unit_price), 0) as signed) unit_price, sum(cnt) cnt from("
    sql += " select * from apt_sale_stats where 1 = 1"
    if from_ym != "":
        sql += " and ym >= '" + from_ym + "'"
    if to_ym != "":
        sql += " and ym <= '" + to_ym + "'"
    if region != "":
        sql += " and region = '" + region + "'"
    if dong != "":
        sql += " and dong = '" + dong + "'"
    if area_type != "":
        sql += " and area_type = '" + area_type + "'"
    if ages != "" and age_sign != "":
        sql += " and " + str(date.today().year - int(ages)) + age_sign + " made_year"
    sql += " ) a  group by ym"
    sql += " order by ym"
    print(sql)
    with app.engine.connect() as connection:
        result = connection.execute(text(sql))

    rows=result.fetchall()            
    json_data = { 'labels': [], 'data':[] }

    spreadDataForYM(rows, 'ym', 'unit_price', json_data['data'], json_data['labels'])

    json_return=json.dumps(json_data)   #string #json
 
    return jsonify(json_return)


