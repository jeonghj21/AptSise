from flask import Flask, request, render_template, json, jsonify
from sqlalchemy import create_engine, text
from json import JSONEncoder
from datetime import date
import datetime

app = Flask(__name__)
app.config.from_envvar('FLASK_CONFIG')

app.engine = create_engine(app.config['DB_URL'], encoding = 'utf-8')

INSERT_ACCESS_LOG = text("insert into access_log values(:ip, str_to_date(:dt, '%Y%m%d%H%i%S'))")
SELECT_LAST_JOB = """
select job_key, ifnull(DATE_FORMAT(end_dt, '%Y/%m/%d %H:%i:%s'), '') end_dt
	 , case when result='Y' then '완료' when result = 'N' then '오류' else '진행중' end status
  from job_log
  where 
 order by start_dt desc
 limit 1
"""
SELECT_LAST_JOB = """
	select a.job_param, ifnull(b.job_key, a.job_key) job_key, ifnull(b.end_dt, ifnull(a.end_dt, a.start_dt)) dt
		 , ifnull(b.result, a.result) result, a.get_cnt, a.insert_cnt, a.new_apt_cnt
	  from (
	  	select * from job_log 
		 where start_dt > date_sub(curdate(), interval 5 day) and job_key like 'ITEMS%' 
		 order by start_dt desc limit 1
	  ) a 
	  left outer join job_log b 
	  on b.start_dt > a.start_dt and b.job_key like 'STAT%' and b.job_param = a.job_param
"""

SELECT_LAST_BATCH = """
	select concat(DATE_FORMAT(start_dt, '%Y/%m/%d %H:%i:%s'), ' : ', name, ifnull(comment, '')) batch 
	  from batch_log where start_dt > date_sub(curdate(), interval 5 day) order by id desc limit 1
"""

@app.route("/")
def index():
	now = datetime.datetime.now()
	result = {}
	try:
		with app.engine.connect() as conn:
			conn.execute(INSERT_ACCESS_LOG, ip=request.remote_addr, dt=now.strftime('%Y%m%d%H%M%S'))
			res = conn.execute(text(SELECT_LAST_JOB))
			for r in res:
				print(r)
				result['job_param'] = r['job_param']
				result['job_key'] = r['job_key']
				result['dt'] = r['dt']
				result['status'] = r['result']

			res = conn.execute(text(SELECT_LAST_BATCH))
			for r in res:
				result['batch'] = r['batch']

	except Exception as e:
		print(str(e))

	return render_template('index.html', result=result, dt=now.timestamp(), version=app.config['VERSION'])

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


def getAddConditions( params, required = {}, ignored = {} ):

	empty_params = []
	for param in params:
		if params[param] == "":
			empty_params.append(param)

	for param in empty_params:
		del params[param]

	add_conditions = ""
	if len(ignored) == 0 or 'danji' not in ignored:
		add_conditions += " and ifnull(danji_flag, 'N') = " + ("'" + params['danji'] + "'" if 'danji' in params else "ifnull(danji_flag," + "'N')")
	if 'from_ym' in params and (len(required) == 0 or 'from_ym' in required) and (len(ignored) == 0 or 'from_ym' not in ignored):
		add_conditions += " and ym >= '" + params['from_ym'] + "'"
	if 'to_ym' in params and (len(required) == 0 or 'to_ym' in required) and (len(ignored) == 0 or 'to_ym' not in ignored):
		add_conditions += " and ym <= '" + params['to_ym'] + "'"
	if 'base_ym' in params and (len(required) == 0 or 'base_ym' in required) and (len(ignored) == 0 or 'base_ym' not in ignored):
		add_conditions += " and ym = '" + params['base_ym'] + "'"
	if 'region_key' in params and (len(required) == 0 or 'region_key' in required) and (len(ignored) == 0 or 'region_key' not in ignored):
		add_conditions += " and region_key = '" + params['region_key'] + "'"
	if 'level' in params and (len(required) == 0 or 'level' in required) and (len(ignored) == 0 or 'level' not in ignored):
		add_conditions += " and level = " + str(params['level'])

	area_type = ""
	if 'area_type' in params and (len(required) == 0 or 'area_type' in required) and (len(ignored) == 0 or 'area_type' not in ignored):
		add_conditions += " and ("
		area_type = params['area_type']
	pos = 0
	while (len(area_type) > pos):
		if (pos > 0):
			add_conditions += " or "
		add_conditions += " area_type = '" + area_type[pos:pos+2] + "'"
		pos = pos + 2
	if pos > 0:
		add_conditions += ")"
	if 'ages' in params and 'age_sign' in params and (len(required) == 0 or ('ages' in required and 'age_sign' in required)): 
		add_conditions += " and " + str(date.today().year - int(params['ages'])) + params['age_sign'] + " made_year"

	print("getAddConditions : " + add_conditions)
	return add_conditions 

SELECT_APT_SALE_LIST = "select b.apt_id, b.ym, cast(a.price as double) price, b.price ma, a.uprice uprice, b.uprice uma, a.cnt"

SELECT_APT_SALE_MA = """
			 select ym, apt_id, round(sum(unit_price* cnt) / sum(cnt), 2) uprice, round(sum(price* cnt) / sum(cnt), 2) price
			   from apt_ma_new 
			  where apt_id = :apt
"""

SELECT_APT_SALE_MA_GROUP_BY = " group by apt_id, ym"

SELECT_APT_SALE = """
	 	 select apt_id, ym, round(avg(price/(area/3.3)), 2) uprice, round(avg(price), 2) price, count(*) cnt 
		   from apt_sale_items 
		  where apt_id = :apt 
"""

SELECT_APT_SALE_GROUP_BY = " group by ym"

SELECT_APT_SALE_JOIN_ON = " on b.apt_id =a.apt_id and a.ym = b.ym order by b.ym"

@app.route("/getSale")
def getSale():
	params = request.args.to_dict()
	apt = params['apt']

	add_conditions = getAddConditions(params, { 'from_ym', 'to_ym' }, { 'danji' })

	sql = SELECT_APT_SALE_LIST + " from (" + SELECT_APT_SALE_MA + add_conditions + SELECT_APT_SALE_MA_GROUP_BY + \
		") b left outer join (" + SELECT_APT_SALE + add_conditions + SELECT_APT_SALE_GROUP_BY + ") a " + SELECT_APT_SALE_JOIN_ON 

	print(sql)
	with app.engine.connect() as connection:
		result = connection.execute(text(sql), apt=apt)

	rows=result.fetchall()            
	json_data = { 'labels': [], 'price':[], 'ma':[], 'uprice':[], 'uma':[], 'cnt':[] }

	spreadDataForYM(rows, 'ym', json_data['labels'], [['price', json_data['price']], ['ma', json_data['ma']]
												   , ['uprice', json_data['uprice']], ['uma', json_data['uma']]
												   , ['cnt', json_data['cnt']]])

	json_return=json.dumps(json_data)   #string #json
 
	return jsonify(json_return)

SELECT_REGIONS_ALL = "select region_key, region_name, upper_region, level from region_info where level <= 3 order by level, upper_region, region_key"
@app.route("/getRegions")
def getRegions():

	regions = []

	with app.engine.connect() as connection:
		result = connection.execute(text(SELECT_REGIONS_ALL))
	
	for res in result:
		region = { 'key': res['region_key'], 'name': res['region_name'], 'level': res['level'], 'upper': res['upper_region'] }
		regions.append(region)

	json_return=json.dumps(regions)   #string #json
 
	return jsonify(json_return)

SELECT_APT_MASTER = """
	select id, apt_name, ifnull(danji_flag, 'N') danji_flag
	 from apt_master
	 where region_key = :region
	 order by apt_name
"""

@app.route("/getApt")
def getApt():

	params = request.args.to_dict()
	region_key = params['region_key']

	with app.engine.connect() as connection:
		result = connection.execute(text(SELECT_APT_MASTER), region=region_key)

	data = []
	for r in result:
		data.append({ 'key': r['id'], 'name': r['apt_name'], 'danji': r['danji_flag'] })
    
	json_return=json.dumps(data)   #string #json
 
	return jsonify(json_return)

SELECT_LIST = """
	select a.ym, convert(a.unit_price, char) uprice, convert(a.cnt, unsigned) cnt, convert(b.unit_price, char) uprice_12ma
		 , convert(a.price, char) price, convert(b.price, char) price_12ma
"""
SELECT_INNER_LIST = "select ym, round(sum(unit_price * cnt)/sum(cnt), 2) unit_price, round(sum(price * cnt)/sum(cnt), 2) price, sum(cnt) cnt"

@app.route("/getSaleStat")
def getSaleStat():
	params = request.args.to_dict()
	region_key = params['region_key']

	add_conditions = getAddConditions(params, {}, { 'base_ym' } )

	sql = SELECT_LIST + " from (" + SELECT_INNER_LIST + " from apt_sale_stats where 1 = 1 " 
	sql += add_conditions + " group by ym )" 
	sql += "a, (" + SELECT_INNER_LIST + " from apt_region_ma where 1 = 1 "
	sql += add_conditions + " group by ym ) b" 

	sql += " where a.ym = b.ym "
	sql += " order by ym"
	print(sql)
	with app.engine.connect() as connection:
		result = connection.execute(text(sql))

	rows=result.fetchall()            
	json_data = { 'labels': [], 'price':[], 'cnt':[], 'ma':[], 'uprice':[], 'uma':[] }

	spreadDataForYM(rows, 'ym', json_data['labels']
					, [['uprice', json_data['uprice']]
					, ['cnt', json_data['cnt']]
					, ['uprice_12ma', json_data['uma']]
					, ['price', json_data['price']]
					, ['price_12ma', json_data['ma']]])

	json_return=json.dumps(json_data)   #string #json
 
	return jsonify(json_return)


@app.route("/getAptSale")
def getAptSale():
	params = request.args.to_dict()
	apt = params['apt']
	sql = "select date_format(saled, '%Y-%m-%d') dt, area, floor, format(price,0) price from apt_sale_items"
	sql += " where apt_id = " + apt 
	sql += getAddConditions(params, { 'base_ym', 'area_type' }, { 'danji' })
	sql += " order by saled"
	print(sql)
	with app.engine.connect() as connection:
		result = connection.execute(text(sql))

	rows=result.fetchall()            

	data = []
	for r in rows:
		data.append([ r['dt'], r['area'], r['floor'], r['price'] ])

	json_return=json.dumps(data)   #string #json
 
	return jsonify(json_return)


@app.route("/getSaleStatTotal")
def getSaleStatTotal():
	params = request.args.to_dict()
	"""
	danji = params['danji']
	from_ym = params['from_ym']
	to_ym = params['to_ym']
	area_type = params['area_type']
	ages = params['ages']
	age_sign = params['age_sign']
	"""
	
	sql = " select ym, cast(sum(cnt) as signed) cnt, "
	sql += " cast(round(avg(unit_price), 0) as signed) unit_price, cast(round(avg(unit_price_12ma), 0) as signed) unit_price_12ma, "
	sql += " cast(round(avg(price), 0) as signed) price, cast(round(avg(price_12ma), 0) as signed) price_12ma"
	sql += " from("
	sql += " select * from apt_sale_stats where 1 = 1"
	
	sql += getAddConditions(params, {})
	sql += " ) a  group by ym"
	sql += " order by ym"

	print(sql)

	with app.engine.connect() as connection:
		result = connection.execute(text(sql))

	rows=result.fetchall()            
	json_data = { 'labels': [], 'data':[], 'cnt':[], 'ma':[] }

	spreadDataForYM(rows, 'ym', json_data['labels']
					, [['unit_price', json_data['data1']]
					, ['cnt', json_data['cnt']]
					, ['unit_price_12ma', json_data['ma1']]
					, ['price', json_data['data2']]
					, ['price_12ma', json_data['ma2']]])

	json_return=json.dumps(json_data)   #string #json
 
	return jsonify(json_return)


PAGE_SIZE = 30
def getRankCommon(request, sqlarr, requiredParams):

	params = request.args.to_dict()
	region = params['region_key']

	cond = getAddConditions(params, requiredParams)

	sql = ""
	for i in range(len(sqlarr)-1):
		sql = sql + sqlarr[i] + cond

	sql = sql + sqlarr[len(sqlarr)-1]

	base_ym = params['base_ym']
	
	years = params['years']
	orderby = params['orderby']
	if orderby != 'name':
		orderby = orderby + " desc"

	page = 1
	if 'page' in params:
		page = int(params['page'])

	if page == -1:
		sql = sql + " order by " + orderby
	else:
		sql = sql + " order by " + orderby + " limit " + str(PAGE_SIZE+1) + " offset " + str((page-1)*PAGE_SIZE)
	
	stmt = text(sql)
	stmt = stmt.bindparams(
			base_ym = base_ym, 
			mm = int(years)*12, 
			region = region
	)

	with app.engine.connect() as connection:
		result = connection.execute(stmt)

	print(sql + ", base_ym="+base_ym+", mm="+str(int(years)*12) + ", region="+region + ", rowcount="+str(result.rowcount))
	json_data = { 'labels': [], 'rate':[], 'price':[], 'before_price':[], 'urate':[], 'uprice':[], 'before_uprice':[], 'has_more':False, 'region_key': [], 'apt': [] }
	i = 0
	for r in result:
		i = i + 1
		if page > 0 and i > PAGE_SIZE:
			json_data['has_more'] = True
			break

		json_data['labels'].append(r['name'])
		json_data['rate'].append(r['rate'])
		json_data['price'].append(r['price'])
		json_data['before_price'].append(r['before_price'])
		json_data['urate'].append(r['urate'])
		json_data['uprice'].append(r['uprice'])
		json_data['before_uprice'].append(r['before_uprice'])
		json_data['region_key'].append(r['region_key'])
		json_data['apt'].append(r['apt'])

	json_return=json.dumps(json_data)   #string #json

	return json_return

SELECT_CHANGE_RATE_REGION = ["""
	select a.*, a.region_key, '' apt
	  from (
		select r.region_name name, convert(a.uprice, char) uprice, convert(b.before_uprice, char) before_uprice
				, convert(a.price, char) price, convert(b.before_price, char) before_price, a.region_key
				  , convert(round((uprice / before_uprice)*100, 2), char) urate, convert(round((price / before_price)*100, 2), char) rate
		 from (
		 	 select a.region_key
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) uprice
			 	  , round(sum(a.price * a.cnt) / sum(a.cnt), 2) price
				  from apt_region_ma a, region_info r
				  where a.ym = :base_ym
				    and r.upper_region = :region
					and a.region_key = r.region_key
""",
"""
				  group by a.region_key
			) a, (
		 	 select a.region_key
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) before_uprice
			 	  , round(sum(a.price * a.cnt) / sum(a.cnt), 2) before_price
				  from apt_region_ma a, region_info r
				  where a.ym = date_format(date_sub(str_to_date(concat(:base_ym, '01'), '%Y%m%d'), interval :mm month), '%Y%m')
				    and r.upper_region = :region
					and a.region_key = r.region_key
""",
"""
				  group by a.region_key
			) b, region_info r 
		  where a.region_key = b.region_key
		    and a.region_key = r.region_key
	) a
"""]

@app.route("/getRankByRegion")
def getRankByRegion():

	json_return = getRankCommon(request, SELECT_CHANGE_RATE_REGION, { 'ages', 'age_sign', 'area_type', 'level' })

	return jsonify(json_return)


SELECT_CHANGE_RATE_APT = ["""
	select a.*, a.region_key, a.apt_id apt
	  from (
		select concat(d.region_name, ' ', a.apt_name) name, a.apt_id, a.price, b.before_price, d.region_key
				  , round((price / before_price) * 100, 2) rate, a.uprice, b.before_uprice, round((uprice / before_uprice) * 100, 2) urate  
		 from (
		 	 select a.apt_id
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) uprice
			 	  , round(sum(a.price * a.cnt) / sum(a.cnt), 2) price
				  , b.region_key
				  , b.apt_name
				  from apt_ma_new a, apt_master b
				  where a.ym = :base_ym
					and a.apt_id = b.id
					and (
						b.region_key = :region
					 or b.region_key in (
					 		select region_key from region_info 
							 where upper_region = :region
						)
					 or b.region_key in (
					 		select region_key from region_info 
					 		 where upper_region in (
							 		select region_key from region_info 
									 where upper_region = :region
							)
						)
					)
""",
"""
				  group by a.apt_id
			) a, (
		 	 select a.apt_id
			 	  , round(sum(a.unit_price * a.cnt) / sum(a.cnt), 2) before_uprice
			 	  , round(sum(a.price * a.cnt) / sum(a.cnt), 2) before_price
				  from apt_ma_new a, apt_master b
				  where a.ym = date_format(date_sub(str_to_date(concat(:base_ym, '01'), '%Y%m%d'), interval :mm month), '%Y%m')
					and a.apt_id = b.id
					and (
						b.region_key = :region
					 or b.region_key in (
					 		select region_key from region_info 
							 where upper_region = :region
						)
					 or b.region_key in (
					 		select region_key from region_info 
					 		 where upper_region in (
							 		select region_key from region_info 
									 where upper_region = :region
							)
						)
					)
""",
"""
				  group by a.apt_id
		 ) b, region_info d       
	 where a.apt_id = b.apt_id
	   and a.region_key = d.region_key
	) a
	where 1 = 1
"""]

@app.route("/getRankByApt")
def getRankByApt():

	json_return = getRankCommon(request, SELECT_CHANGE_RATE_APT, { 'danji', 'ages', 'age_sign', 'area_type' })

	return jsonify(json_return)


SELECT_QBOX = """
	select 1 max_min, price_gubun, a.ym, max_price, min_price, 1q_price, 3q_price, median_price, avg_price
		 , DATE_FORMAT(saled, '%Y/%m/%d') saled, price, area, floor, apt_id, apt_name, made_year
   	  from apt_qbox_stats a, apt_sale_items s, apt_master m
  	 where a.region_key = :region
 	   and a.level = :level
	   and a.danji_flag = :danji
 	   and a.ym between :from_ym and :to_ym
	   and s.id = a.max_sale_id
	   and s.apt_id = m.id
	union
	select 2 max_min, price_gubun, a.ym, max_price, min_price, 1q_price, 3q_price, median_price, avg_price
		 , DATE_FORMAT(saled, '%Y/%m/%d') saled, price, area, floor, apt_id, apt_name, made_year
   	  from apt_qbox_stats a, apt_sale_items s, apt_master m
  	 where a.region_key = :region
 	   and a.level = :level
	   and a.danji_flag = :danji
 	   and a.ym between :from_ym and :to_ym
	   and s.id = a.min_sale_id
	   and s.apt_id = m.id
  order by price_gubun, ym, max_min
"""

@app.route("/getBoxPlot")
def getBoxPlot():
	params = request.args.to_dict()
	danji = params['danji']
	if danji == "":
		danji = 'N'

	print(SELECT_QBOX)
	with app.engine.connect() as connection:
		result = connection.execute(text(SELECT_QBOX), upper=params['upper'], region=params['region_key'], level=params['level'], \
													   danji=danji, from_ym=params['from_ym'], to_ym=params['to_ym'])
	rows=result.fetchall()            

	data = { 'labels': [], 'data': [] }
	before_gubun = ''
	data_idx = 0
	data['data'].append([])
	for r in rows:
		if before_gubun != '' and before_gubun != r['price_gubun']:
			data_idx = data_idx + 1
			data['data'].append([])
		if data_idx == 0 and (len(data['labels']) == 0 or data['labels'][len(data['labels'])-1] != r['ym']):
			data['labels'].append(r['ym'])
		data['data'][data_idx].append({ 'price_gubun': r['price_gubun'], 'ym': r['ym'], 'max_price': r['max_price'], 'min_price': r['min_price'], \
								'1q_price': r['1q_price'], '3q_price': r['3q_price'], 'median_price': r['median_price'], 'avg_price': r['avg_price'], \
		         				'saled': r['saled'], 'price': r['price'], 'area': r['area'], 'floor': r['floor'], 'apt_id': r['apt_id'], \
								'apt_name': r['apt_name'], 'made_year': r['made_year'] })
		before_gubun = r['price_gubun']

	json_return=json.dumps(data)   #string #json
 
	return jsonify(json_return)


