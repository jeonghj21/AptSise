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
			if ym_data[data_label].__contains__(min_ym):
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

SELECT_APT_SALE_AND_MA = """
	select b.ma_id apt_id, b.ym, a.unit_price price, b.unit_price ma, a.cnt 
		 from (
			 select ym, ma_id, round(sum(unit_price * cnt) / sum(cnt), 2) unit_price
			   from apt_sale_ma_new 
			  where ma_type = '1' and ma_id = :apt_id1 and ma = 12
			  group by ma_id, ym
		 ) b 
	 left outer join (
	 	 select apt_id, ym, round(avg(price/(area/3.3)), 2) unit_price, count(*) cnt 
		   from apt_sale_new 
		  where apt_id = :apt_id2 
		  group by ym
		 ) a
	 on b.ma_id =a.apt_id and a.ym = b.ym order by b.ym
"""

SELECT_APT_SALE = """
	select date_format(saled, '%Y%m') ym
	 , round(sum(price)/(sum(area)/3.3),1) price
	 , count(id) count
	 from apt_sale_new s
	 where apt_id=:apt
	 group by date_format(saled, '%Y%m') 
	 order by date_format(saled, '%Y%m')
"""

@app.route("/getSale")
def getSale():
	params = request.args.to_dict()
	apt = params['apt']

	sql = text(SELECT_APT_SALE_AND_MA)
	print(sql)
	with app.engine.connect() as connection:
		result = connection.execute(sql, apt_id1=apt, apt_id2=apt)

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
	complex_only = params['complex']
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
	if complex_only != "":
		add_conditions += " and complex_flag = '" + complex_only + "'"
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
	sql += "a, (" + SELECT_INNER_LIST + " from apt_sale_ma_new where ma_type = '2' and ma = 12 "
	sql += "and ma_id = concat('" + region + "', '" + dong + "') "
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
	complex_only = params['complex']
	from_ym = params['from_ym']
	to_ym = params['to_ym']
	area_type = params['area_type']
	ages = params['ages']
	age_sign = params['age_sign']

	sql = " select ym, cast(round(avg(unit_price), 0) as signed) unit_price, "
	sql += " cast(sum(cnt) as signed) cnt, cast(round(avg(unit_price_12ma), 0) as signed) unit_price_12ma"
	sql += " from("
	sql += " select * from apt_sale_stats_new where 1 = 1"
	if complex_only != "":
		sql += " and complex_flag = '" + complex_only + "'"
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

	with app.engine.connect() as connection:
		result = connection.execute(text(sql))

	rows=result.fetchall()            
	json_data = { 'labels': [], 'data':[], 'cnt':[], 'ma':[] }

	spreadDataForYM(rows, 'ym', json_data['labels'], [['unit_price', json_data['data']], ['cnt', json_data['cnt']], ['unit_price_12ma', json_data['ma']]])

	json_return=json.dumps(json_data)   #string #json
 
	return jsonify(json_return)


PAGE_SIZE = 30
def getRankCommon(request, sql, addConditions = {}):

	for key, value in addConditions.items():
		sql = sql + " and " + key + " = '" + value + "'"

	params = request.args.to_dict()
	base_ym = params['base']
	years = params['years']
	orderby = params['orderby']
	page = 1
	if params.__contains__('page'):
		page = int(params['page'])

	if page == -1:
		sql = text(sql + " order by " + orderby + " desc")
	else:
		sql = text(sql + " order by " + orderby + " desc limit " + str(PAGE_SIZE+1) + " offset " + str((page-1)*PAGE_SIZE))
	print(sql)
	with app.engine.connect() as connection:
		result = connection.execute(sql, base_ym=base_ym, mm = int(years)*12)

	json_data = { 'labels': [], 'data':[], 'price':[], 'before_price':[], 'has_more':False }
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


	json_return=json.dumps(json_data)   #string #json

	return json_return

SELECT_CHANGE_RATE_REGION = """
	select r.region_name name, a.* 
		 from (
		 	 select substr(a.ma_id, 1, 5) region
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) price
				  , round(sum(b.unit_price * b.cnt) / sum(b.cnt), 2) before_price
				  , round((sum(a.unit_price * a.cnt) / sum(a.cnt)) / (sum(b.unit_price * b.cnt) / sum(b.cnt)), 2) rate  
				  from apt_sale_ma_new a
				  	 , apt_sale_ma_new b 
				  where a.ma_type = '2'
				    and b.ma_type = '2'
					and a.ma = 12
					and b.ma = 12
					and a.ym = :base_ym
				    and b.ym = date_format(date_sub(str_to_date(concat(a.ym, '01'), '%Y%m%d'), interval :mm month), '%Y%m') 
					and substr(a.ma_id, 1, 5) = substr(b.ma_id, 1, 5)
				  group by substr(a.ma_id, 1, 5)
			) a, ref_region r 
		  where a.region = r.region_cd
"""

@app.route("/getRankByRegion")
def getRankByRegion():

	json_return = getRankCommon(request, SELECT_CHANGE_RATE_REGION)

	return jsonify(json_return)


SELECT_CHANGE_RATE_DONG = """
	select concat(r.region_name, ' ', d.dong_name) name, a.*          
		 from (              
		 	 select substr(a.ma_id, 1, 5) region, substr(a.ma_id, 6, 5) dong   
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) price
				  , round(sum(b.unit_price * b.cnt) / sum(b.cnt), 2) before_price
				  , round((sum(a.unit_price * a.cnt) / sum(a.cnt)) / (sum(b.unit_price * b.cnt) / sum(b.cnt)), 2) rate  
			   from apt_sale_ma_new a
				  , apt_sale_ma_new b 
			  where a.ma_type = '2'
				and b.ma_type = '2'
				and a.ma = 12
				and b.ma = 12
			    and a.ym = :base_ym                   
			 	and b.ym = date_format(date_sub(str_to_date(concat(a.ym, '01'), '%Y%m%d'), interval :mm month), '%Y%m')                     
				and a.ma_id = b.ma_id
		      group by a.ma_id
		  ) a, ref_region r, apt_dong d           
		 where a.region = r.region_cd 
		   and a.region = d.region_cd 
		   and a.dong = d.dong_cd           
"""

@app.route("/getRankByDong")
def getRankByDong():

	addConditions = {}

	params = request.args.to_dict()
	if params.__contains__('region'):
		region = params['region']
		addConditions['a.region'] = region

	json_return = getRankCommon(request, SELECT_CHANGE_RATE_DONG, addConditions)

	return jsonify(json_return)


SELECT_CHANGE_RATE_APT = """
	select concat(d.dong_name, ' ', m.apt_name) name, a.*          
		 from (              
		 	 select a.ma_id apt_id       
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) price                   
				  , round(sum(b.unit_price * b.cnt) / sum(b.cnt), 2) before_price
         		  , round((sum(a.unit_price * a.cnt) / sum(a.cnt)) / (sum(b.unit_price * b.cnt) / sum(b.cnt)), 2) rate                   
			   from apt_sale_ma_new a
         		  , apt_sale_ma_new b                   
			  where a.ma_type = '1'
			    and b.ma_type = '1'
				and a.ma = 12
				and b.ma = 12
				and a.ym = :base_ym              
			    and b.ym = date_format(date_sub(str_to_date(concat(a.ym, '01'), '%Y%m%d'), interval :mm month), '%Y%m')
		        and a.ma_id = b.ma_id          
			  group by a.ma_id         
		 ) a, apt_master_new m, apt_dong d       
	 where a.apt_id = m.id  
	   and m.region = d.region_cd
	   and m.dong = d.dong_cd
"""

@app.route("/getRankByApt")
def getRankByApt():

	addConditions = {}

	params = request.args.to_dict()
	if params.__contains__('region'):
		addConditions['m.region'] = params['region']
	if params.__contains__('dong'):
		addConditions['m.dong'] = params['dong']

	
	json_return = getRankCommon(request, SELECT_CHANGE_RATE_APT, addConditions)

	return jsonify(json_return)

