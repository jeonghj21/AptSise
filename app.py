from flask import Flask, request, render_template, json, jsonify
from sqlalchemy import create_engine, text
from json import JSONEncoder
from datetime import date
import datetime

app = Flask(__name__)
app.config.from_envvar('FLASK_CONFIG')

app.engine = create_engine(app.config['DB_URL'], encoding = 'utf-8')

INSERT_ACCESS_LOG = text("insert into access_log values(:ip, str_to_date(:dt, '%Y%m%d%H%i%S'))")

@app.route("/")
def index():
	now = datetime.datetime.now()
	try:
		with app.engine.connect() as conn:
			conn.execute(INSERT_ACCESS_LOG, ip=request.remote_addr, dt=now.strftime('%Y%m%d%H%M%S'))
	except Exception as e:
		print(str(e))

	return render_template('index.html')

def spreadDataForYM(rows, ym_label, labels, data_list):
	if len(rows) == 0:
		print("spreadDataForYM : rows = 0")
		return

	min_ym = ''
	max_ym = ''
	ym_data = {}
	for r in rows:
		ym = r[ym_label]
		if min_ym == '': 
			min_ym = ym
		max_ym = ym
		for data in data_list:
			data_label = data[0]
			if data_label not in ym_data.keys():
				ym_data[data_label] = {}
			ym_data[data_label][ym] = r[data_label]
    
	while min_ym <= max_ym:
		labels.append(min_ym)
		for data in data_list:
			data_label = data[0]
			if min_ym in ym_data[data_label]:
				data[1].append(ym_data[data_label][min_ym])
			else:
				data[1].append(None)
		yy = int(min_ym[:4])
		mm = int(min_ym[4:6])
		if mm == 12:
			yy = yy + 1
			mm = 1
		else:
			mm = mm + 1
		min_ym = str(yy) + str(mm).zfill(2)

SELECT_APT_SALE_LIST = "select b.apt_id, b.ym, a.unit_price price, b.unit_price ma, a.cnt"

SELECT_APT_SALE_MA = """
			 select ym, apt_id, round(sum(unit_price * cnt) / sum(cnt), 2) unit_price
			   from apt_ma_new 
			  where apt_id = :apt
"""

SELECT_APT_SALE_MA_GROUP_BY = " group by apt_id, ym"

SELECT_APT_SALE = """
	 	 select apt_id, ym, round(avg(price/(area/3.3)), 2) unit_price, count(*) cnt 
		   from apt_sale_new 
		  where apt_id = :apt 
"""

SELECT_APT_SALE_GROUP_BY = " group by ym"

SELECT_APT_SALE_JOIN_ON = " on b.apt_id =a.apt_id and a.ym = b.ym order by b.ym"

@app.route("/getSale")
def getSale():
	params = request.args.to_dict()
	apt = params['apt']

	from_ym = params['from_ym']
	to_ym = params['to_ym']
	area_type = params['area_type']

	add_conditions = ""
	if from_ym != "":
		add_conditions += " and ym >= '" + from_ym + "'"
	if to_ym != "":
		add_conditions += " and ym <= '" + to_ym + "'"

	if area_type != "":
		add_conditions += " and ("
	pos = 0
	while (len(area_type) > pos):
		if (pos > 0):
			add_conditions += " or "
		add_conditions += " area_type = '" + area_type[pos:pos+2] + "'"
		pos = pos + 2
	if area_type != "":
		add_conditions += ")"

	sql = SELECT_APT_SALE_LIST + " from (" + SELECT_APT_SALE_MA + SELECT_APT_SALE_MA_GROUP_BY + \
		") b left outer join (" + SELECT_APT_SALE + add_conditions + SELECT_APT_SALE_GROUP_BY + ") a " + SELECT_APT_SALE_JOIN_ON 

	print(sql)
	with app.engine.connect() as connection:
		result = connection.execute(text(sql), apt=apt)

	rows=result.fetchall()            
	json_data = { 'labels': [], 'data':[], 'maData':[], 'cnt':[] }

	spreadDataForYM(rows, 'ym', json_data['labels'], [['price', json_data['data']], ['ma', json_data['maData']], ['cnt', json_data['cnt']]])

	json_return=json.dumps(json_data)   #string #json
 
	return jsonify(json_return)

SELECT_REGIONS = "select region_cd, region_name from ref_region"
SELECT_DONGS = "select * from apt_dong where region_cd = :region and valid = 'Y' order by dong_name"
@app.route("/getDong")
def getDong():

	data = []
	regions = []

	with app.engine.connect() as connection:
		result = connection.execute(text(SELECT_REGIONS))
	for res in result:
		regions.append([res['region_cd'], res['region_name']])

	data.append(regions)

	dongs = {}
	for r in regions:
		dongs_in_r = []
		with app.engine.connect() as connection:
			result = connection.execute(text(SELECT_DONGS), region=r[0])
		for res in result:
			dongs_in_r.append([res['dong_cd'], res['dong_name']])
		dongs[r[0]] = dongs_in_r

	data.append(dongs)
    
	json_return=json.dumps(data)   #string #json
 
	return jsonify(json_return)

SELECT_APT_MASTER = """
	select id, apt_name, k_apt_id
	 from apt_master_new
	 where region = :region
	  and dong = :dong
	 order by apt_name
"""

@app.route("/getApt")
def getApt():

	params = request.args.to_dict()
	region = params['region']
	dong = params['dong']

	with app.engine.connect() as connection:
		result = connection.execute(text(SELECT_APT_MASTER), region=region, dong=dong)

	data = []
	for r in result:
		data.append([ r['id'], r['apt_name'], r['k_apt_id'] ])
    
	json_return=json.dumps(data)   #string #json
 
	return jsonify(json_return)

SELECT_LIST = "select a.ym, convert(a.unit_price, char) unit_price, convert(a.cnt, unsigned) cnt, convert(b.unit_price, char) unit_price_12ma"
SELECT_INNER_LIST = "select ym, round(sum(unit_price * cnt)/sum(cnt), 2) unit_price, sum(cnt) cnt"

@app.route("/getSaleStat")
def getSaleStat():
	params = request.args.to_dict()
	danji_only = params['danji']
	from_ym = params['from_ym']
	to_ym = params['to_ym']
	region = params['region']
	if region == "":
		region = "11000"
	dong = params['dong']
	if dong == "":
		dong = "00000"
	area_type = params['area_type']
	ages = params['ages']
	age_sign = params['age_sign']

	add_conditions = ""
	if danji_only != "":
		add_conditions += " and danji_flag = '" + danji_only + "'"
	if from_ym != "":
		add_conditions += " and ym >= '" + from_ym + "'"
	if to_ym != "":
		add_conditions += " and ym <= '" + to_ym + "'"

	if area_type != "":
		add_conditions += " and ("
	pos = 0
	while (len(area_type) > pos):
		if (pos > 0):
			add_conditions += " or "
		add_conditions += " area_type = '" + area_type[pos:pos+2] + "'"
		pos = pos + 2
	if area_type != "":
		add_conditions += ")"
	if ages != "" and age_sign != "":
		add_conditions += " and " + str(date.today().year - int(ages)) + age_sign + " made_year"

	sql = SELECT_LIST + " from (" + SELECT_INNER_LIST + " from apt_sale_stats_new where 1 = 1 " 
	sql += "and region='" + region + "' and dong = '" + dong + "'"
	sql += add_conditions + " group by ym )" 
	sql += "a, (" + SELECT_INNER_LIST + " from apt_region_ma_new where 1 = 1 "
	sql += "and region='" + region + "' and dong = '" + dong + "'"
	sql += add_conditions + " group by ym ) b" 

	sql += " where a.ym = b.ym "
	sql += " order by ym"
	print(sql)
	with app.engine.connect() as connection:
		result = connection.execute(text(sql))

	rows=result.fetchall()            
	json_data = { 'labels': [], 'data':[], 'cnt':[], 'ma':[] }

	spreadDataForYM(rows, 'ym', json_data['labels'], [['unit_price', json_data['data']], ['cnt', json_data['cnt']], ['unit_price_12ma', json_data['ma']]])

	json_return=json.dumps(json_data)   #string #json
 
	return jsonify(json_return)


@app.route("/getAptSale")
def getAptSale():
	params = request.args.to_dict()
	apt = params['apt']
	sql = "select date_format(saled, '%Y-%m-%d') dt, area, floor, format(price,0) price from apt_sale_new"
	sql += " where apt_id = " + apt 
	if 'ym' in params:
		sql += " and ym = '" + params['ym'] + "'"
	sql += " order by saled"
	with app.engine.connect() as connection:
		result = connection.execute(text(sql))

	rows=result.fetchall()            

	data = []
	for r in rows:
		data.append([ r['dt'], r['area'], r['floor'], r['price'] ])

	json_return=json.dumps(data)   #string #json
 
	return jsonify(json_return)


@app.route("/getKBIndex")
def getKBIndex():
    params = request.args.to_dict()
    from_ym = params['from_ym']
    to_ym = params['to_ym']
    region = params['region']

    sql = " select ym, index_val*40 val from kb_region_index_ym"
    sql += " where region = '" + region + "'"
    if from_ym != "":
        sql += " and ym >= '" + from_ym + "'"
    if to_ym != "":
        sql += " and ym <= '" + to_ym + "'"

    with app.engine.connect() as connection:
        result = connection.execute(text(sql))

    rows=result.fetchall()            
    json_data = { 'labels': [], 'data':[] }

    for r in rows:
        json_data['labels'].append(r[0]);
        json_data['data'].append(r[1]);

    json_return=json.dumps(json_data)   #string #json
 
    return jsonify(json_return)

INSERT_USER_REMARKS = text("insert into user_remarks values(null, :ip, :browser, str_to_date(:dt, '%Y%m%d%H%i%S'), :remarks, :email)")

@app.route("/remarks")
def remarks():
	now = datetime.datetime.now()
	try:
		with app.engine.connect() as conn:
			conn.execute(INSERT_USER_REMARKS, ip=request.remote_addr, dt=now.strftime('%Y%m%d%H%M%S'))
	except Exception as e:
		print(str(e))

	return render_template('index.html')

@app.route("/getSaleStatTotal")
def getSaleStatTotal():
	params = request.args.to_dict()
	danji_only = params['danji']
	from_ym = params['from_ym']
	to_ym = params['to_ym']
	area_type = params['area_type']
	ages = params['ages']
	age_sign = params['age_sign']

	sql = " select ym, cast(round(avg(unit_price), 0) as signed) unit_price, "
	sql += " cast(sum(cnt) as signed) cnt, cast(round(avg(unit_price_12ma), 0) as signed) unit_price_12ma"
	sql += " from("
	sql += " select * from apt_sale_stats_new where 1 = 1"
	if danji_only != "":
		sql += " and danji_flag = '" + danji_only + "'"
	if from_ym != "":
		sql += " and ym >= '" + from_ym + "'"
	if to_ym != "":
		sql += " and ym <= '" + to_ym + "'"

	if area_type != "":
		sql += " and ("
	pos = 0
	while (len(area_type) > pos):
		if (pos > 0):
			sql += " or "
		sql += " area_type = '" + area_type[pos:pos+2] + "'"
		pos = pos + 2
	if ages != "" and age_sign != "":
		sql += " and " + str(date.today().year - int(ages)) + age_sign + " made_year"
	sql += " ) a  group by ym"
	sql += " order by ym"

	print(sql)

	with app.engine.connect() as connection:
		result = connection.execute(text(sql))

	rows=result.fetchall()            
	json_data = { 'labels': [], 'data':[], 'cnt':[], 'ma':[] }

	spreadDataForYM(rows, 'ym', json_data['labels'], [['unit_price', json_data['data']], ['cnt', json_data['cnt']], ['unit_price_12ma', json_data['ma']]])

	json_return=json.dumps(json_data)   #string #json
 
	return jsonify(json_return)

def getAddConditions( params ):

	empty_params = []
	for param in params:
		if params[param] == "":
			empty_params.append(param)

	for param in empty_params:
		del params[param]

	add_conditions = ""
	if 'danji' in params:
		add_conditions += " and danji_flag = '" + params['danji'] + "'"
	if 'from_ym' in params:
		add_conditions += " and ym >= '" + params['from_ym'] + "'"
	if 'to_ym' in params:
		add_conditions += " and ym <= '" + params['to_ym'] + "'"
	if 'region' in params:
		add_conditions += " and region = '" + params['region'] + "'"
	if 'dong' in params:
		add_conditions += " and dong = '" + params['dong'] + "'"

	area_type = ""
	if 'area_type' in params:
		add_conditions += " and ("
		area_type = params['area_type']
	pos = 0
	while (len(area_type) > pos):
		if (pos > 0):
			add_conditions += " or "
		add_conditions += " area_type = '" + area_type[pos:pos+2] + "'"
		pos = pos + 2
	if 'area_type' in params:
		add_conditions += ")"
	if 'ages' in params and 'age_sign' in params: 
		add_conditions += " and " + str(date.today().year - int(params['ages'])) + params['age_sign'] + " made_year"

	return add_conditions 


PAGE_SIZE = 30
def getRankCommon(request, sqlarr):

	params = request.args.to_dict()
	
	cond = getAddConditions(params)

	sql = ""
	for i in range(len(sqlarr)-1):
		sql = sql + sqlarr[i] + cond

	sql = sql + sqlarr[len(sqlarr)-1]

	base_ym = params['base']
	years = params['years']
	orderby = params['orderby']
	page = 1
	if 'page' in params:
		page = int(params['page'])

	if page == -1:
		sql = text(sql + " order by " + orderby + " desc")
	else:
		sql = text(sql + " order by " + orderby + " desc limit " + str(PAGE_SIZE+1) + " offset " + str((page-1)*PAGE_SIZE))
	print(sql)
	with app.engine.connect() as connection:
		result = connection.execute(sql, base_ym=base_ym, mm = int(years)*12)

	json_data = { 'labels': [], 'data':[], 'price':[], 'before_price':[], 'has_more':False, 'region': [], 'dong': [], 'apt': [] }
	i = 0
	for r in result:
		i = i + 1
		if page > 0 and i > PAGE_SIZE:
			json_data['has_more'] = True
			break

		json_data['labels'].append(r['name'])
		json_data['data'].append(r['rate'])
		json_data['price'].append(r['price'])
		json_data['before_price'].append(r['before_price'])
		json_data['region'].append(r['region'])
		json_data['dong'].append(r['dong'])
		json_data['apt'].append(r['apt'])


	json_return=json.dumps(json_data)   #string #json

	return json_return

SELECT_CHANGE_RATE_REGION_1 = """
	select a.*, a.region, '' dong, '' apt
	  from (
		select r.region_name name, a.price, b.before_price, a.region
				  , round(price / before_price, 2) rate  
		 from (
		 	 select region
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) price
				  from apt_region_ma_new a
				  where a.ym = :base_ym
"""
SELECT_CHANGE_RATE_REGION_2 = """
				  group by region
			) a, (
		 	 select region
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) before_price
				  from apt_region_ma_new a
				  where a.ym = date_format(date_sub(str_to_date(concat(:base_ym, '01'), '%Y%m%d'), interval :mm month), '%Y%m')
"""
SELECT_CHANGE_RATE_REGION_3 = """
				  group by region
			) b, ref_region r 
		  where a.region = b.region
		    and a.region = r.region_cd
	) a
"""

@app.route("/getRankByRegion")
def getRankByRegion():

	json_return = getRankCommon(request, [SELECT_CHANGE_RATE_REGION_1, SELECT_CHANGE_RATE_REGION_2, SELECT_CHANGE_RATE_REGION_3])

	return jsonify(json_return)


SELECT_CHANGE_RATE_DONG_1 = """
	select a.*, a.region, a.dong, '' apt
	  from (
		select concat(r.region_name, ' ', d.dong_name) name, a.price, b.before_price, a.region, a.dong
				  , round(price / before_price, 2) rate  
		 from (
		 	 select region
			      , dong
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) price
				  from apt_region_ma_new a
				  where a.ym = :base_ym
"""
SELECT_CHANGE_RATE_DONG_2 = """
				  group by a.region, a.dong
			) a, (
		 	 select region
			      , dong
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) before_price
				  from apt_region_ma_new a
				  where a.ym = date_format(date_sub(str_to_date(concat(:base_ym, '01'), '%Y%m%d'), interval :mm month), '%Y%m')
"""
SELECT_CHANGE_RATE_DONG_3 = """
				  group by a.region, a.dong
			) b, ref_region r, apt_dong d 
		  where a.region = b.region
		    and a.dong = b.dong
		    and a.region = r.region_cd
			and a.region = d.region_cd
			and a.dong = d.dong_cd
	) a
	where 1 = 1
"""

@app.route("/getRankByDong")
def getRankByDong():

	json_return = getRankCommon(request, [SELECT_CHANGE_RATE_DONG_1, SELECT_CHANGE_RATE_DONG_2, SELECT_CHANGE_RATE_DONG_3])

	return jsonify(json_return)


SELECT_CHANGE_RATE_APT_1 = """
	select a.*, a.region, a.dong, a.apt_id apt
	  from (
		select concat(d.dong_name, ' ', m.apt_name) name, a.apt_id, a.price, b.before_price, d.region_cd region, d.dong_cd dong
				  , round(price / before_price, 2) rate  
		 from (
		 	 select a.apt_id
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) price
				  from apt_ma_new a, apt_master_new b
				  where a.ym = :base_ym
					and a.apt_id = b.id
"""
SELECT_CHANGE_RATE_APT_2 = """
				  group by a.apt_id
			) a, (
		 	 select a.apt_id
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) before_price
				  from apt_ma_new a, apt_master_new b
				  where a.ym = date_format(date_sub(str_to_date(concat(:base_ym, '01'), '%Y%m%d'), interval :mm month), '%Y%m')
					and a.apt_id = b.id
"""
SELECT_CHANGE_RATE_APT_3 = """
				  group by a.apt_id
		 ) b, apt_master_new m, apt_dong d       
	 where a.apt_id = b.apt_id
	   and a.apt_id = m.id  
	   and m.region = d.region_cd
	   and m.dong = d.dong_cd
	) a
	where 1 = 1
"""

@app.route("/getRankByApt")
def getRankByApt():

	json_return = getRankCommon(request, [SELECT_CHANGE_RATE_APT_1, SELECT_CHANGE_RATE_APT_2, SELECT_CHANGE_RATE_APT_3])

	return jsonify(json_return)

