from flask import Flask, request, render_template, jsonify
from sqlalchemy import create_engine, text
from datetime import date, datetime
import logging
import simplejson as json
from dateutil.relativedelta import relativedelta

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

app = Flask(__name__)
app.config.from_envvar('FLASK_CONFIG')

app.engine = create_engine(app.config['DB_URL'], encoding = 'utf-8')

INSERT_ACCESS_LOG = text("insert into access_log values(:ip, str_to_date(:dt, '%Y%m%d%H%i%S'))")
SELECT_LAST_JOB = """
select job_key, job_param, DATE_FORMAT(ifnull(end_dt, start_dt), '%Y/%m/%d %H:%i:%s') dt
	 , case when result='Y' then '완료' when result = 'N' then '오류' else '진행중' end status
  from job_log
 order by start_dt desc
 limit 1
"""

SELECT_LAST_BATCH = """
	select concat(DATE_FORMAT(start_dt, '%Y/%m/%d %H:%i:%s'), ' : ', name, ifnull(comment, '')) batch 
	  from batch_log where start_dt > date_sub(curdate(), interval 5 day) order by id desc limit 1
"""

@app.route("/")
def index():
	now = datetime.now()
	result = {}
	try:
		with app.engine.connect() as conn:
			conn.execute(INSERT_ACCESS_LOG, ip=request.remote_addr, dt=now.strftime('%Y%m%d%H%M%S'))
			res = conn.execute(text(SELECT_LAST_JOB))
			for r in res:
				result['job_param'] = r['job_param']
				result['job_key'] = r['job_key']
				result['dt'] = r['dt']
				result['status'] = r['status']

			res = conn.execute(text(SELECT_LAST_BATCH))
			for r in res:
				result['batch'] = r['batch']

	except Exception as e:
		print(str(e))

	return render_template('index.html', result=result, dt=now.timestamp(), version=app.config['VERSION'], version_dt=app.config['VERSION_DT'])

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

	with app.engine.connect() as connection:
		result = connection.execute(text(sql), apt=apt)

	rows=result.fetchall()            
	json_data = { 'labels': [], 'price':[], 'ma':[], 'uprice':[], 'uma':[], 'cnt':[] }

	spreadDataForYM(rows, 'ym', json_data['labels'], [['price', json_data['price']], ['ma', json_data['ma']]
												   , ['uprice', json_data['uprice']], ['uma', json_data['uma']]
												   , ['cnt', json_data['cnt']]])

	json_return=json.dumps(json_data)   #string #json
 
	return jsonify(json_return)

SELECT_REGIONS_ALL = "select region_key, region_name, upper_region, level, apt_yn from region_info where level <= 3 order by level, upper_region, region_key"
@app.route("/getRegions")
def getRegions():

	regions = []

	with app.engine.connect() as connection:
		result = connection.execute(text(SELECT_REGIONS_ALL))
	
	for res in result:
		region = { 'key': res['region_key'], 'name': res['region_name'], 'level': res['level'], 'upper': res['upper_region'], 'apt_yn': res['apt_yn'] }
		regions.append(region)

	json_return=json.dumps(regions)   #string #json
 
	return jsonify(json_return)

SELECT_APT_MASTER = """
	select a.id, apt_name, ifnull(danji_flag, 'N') danji_flag
		 , n.id naver_id, n.name naver_name, a.jibun1, a.jibun2, n.road_addr
		from apt_master a 
		left join naver_complex_info n 
			on ifnull(a.naver_id, 0) = n.id    
		where a.region_key = :region
"""

@app.route("/getApt")
def getApt():

	params = request.args.to_dict()
	region_key = params['region_key']

	with app.engine.connect() as connection:
		result = connection.execute(text(SELECT_APT_MASTER), region=region_key)

	data = []
	for r in result:
		data_naver = {'id': r['naver_id'], 'name': r['naver_name'], \
					  'jibun1': r['jibun1'], 'jibun2': r['jibun2'], 'road_addr': r['road_addr']}
		data.append({ 'key': r['id'], 'name': r['apt_name'], 'danji': r['danji_flag'], 'naver': data_naver })
    
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
	select a.*, '' apt
	from(
		select r.region_name name, a.uprice, b.before_uprice
			, a.price, b.before_price, a.region_key
		  	, round((uprice / before_uprice)*100, 2) urate
		  	, round((price / before_price)*100, 2) rate
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

SELECT_REGION_KEY = """
	select region_key from region_info
        where region_name=:region3 and level=3 
		  and upper_region=
            (select region_key from region_info
                where level=2 and region_name=:region2 
				  and upper_region=
                    (select region_key from region_info
                        where level=1 and region_name=:region1)
            )
"""
REGIONs = {'서울시':'서울특별시', '대구시':'대구광역시', '인천시':'인천광역시', '부산시':'부산광역시', \
		   '광주시':'광주광역시', '대전시':'대전광역시', '울산시':'울산광역시', '세종시':'세종특별자치시', \
		   '제주도':'제주특별자치도' }

def adjust_complex_info(data):

	addrs = data['address'].split(' ')
	if len(addrs) < 3:
		return None

	if addrs[2][-1] == '구' and addrs[1][-1] == '시' and len(addrs) > 3:
		addrs[1] = addrs[1][:-1]+addrs[2]
		addrs[2] = addrs[3]

	data['region1'] = addrs[0]
	data['region2'] = addrs[1]
	data['region3'] = addrs[2]

	if data['region1'] in REGIONs:
		data['region1'] = REGIONs[data['region1']]

	tmp = addrs[len(addrs)-1].split('-')
	data['jibun1'] = tmp[0]
	data['jibun2'] = tmp[1] if len(tmp) > 1 else 0

	data['family'] = int(data['family'][:-2])
	data['dong'] = int(data['dong'][2:-1])
	data['made_year'] = int(0 if data['approved'] == '-' else data['approved'][:4])

	tmp = data['areas'].split(' ~ ')
	data['min_area'] = tmp[0][:-1]
	data['max_area'] = tmp[1][:-1] if len(tmp) > 1 else data['min_area']

	with app.engine.connect() as connection:
		result = connection.execute(text(SELECT_REGION_KEY), \
				region1 = data['region1'], region2 = data['region2'], region3 = data['region3'])

	r = result.first()
	if r == None:
		return None

	data['region_key'] = r['region_key']
	


INSERT_NAVER_COMPLEX = """
	insert into naver_complex_info 
		(id, category, name, family_cnt, dong_cnt, made_year, min_area, max_area, region_key, jibun1, jibun2, road_addr)
	values
		(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

KEYs = ['id', 'cate', 'name', 'family', 'dong', 'made_year', 'min_area', 'max_area', \
		'region_key', 'jibun1', 'jibun2', 'road_addr']

@app.route("/saveNaverComplexInfo", methods=["POST"])
def saveNaverComplexInfo():

	data = request.form.to_dict()
	adjust_complex_info(data)

	l = []
	for key in keys:
		l.append(data[key])

	t = tuple(l)
	rows = -1
	try:
		connection = app.engine.raw_connection()
		cursor = connection.cursor()
		cursor.execute(INSERT_NAVER_COMPLEX, t)
		rows = cursor.rowcount
		connection.commit()
		cursor.close()
	except Exception as e:
		print(str(e))
		return jsonify({'result': 'Fail'})
	finally:
		connection.close()

	return jsonify({'result': 'OK'})

SELECT_1M_1Y_COMPARE_COMMON = """
		  from apt_qbox_stats s, region_info r 
		 where s.region_key = r.region_key 
		   and r.upper_region = :region
		   and s.ym = :ym
		   and s.danji_flag = :danji 
		   and s.price_gubun = :price_gubun
		 group by s.region_key
"""

SELECT_1M_1Y_COMPARE_HEAD = """
	select s1.region_key, s1.region_name
		 , cur_uprice, cur_price, cur_cnt
		 , before_1m_uprice, before_1m_price, before_1m_cnt
		 , before_1y_uprice, before_1y_price, before_1y_cnt 
		 , cast(substr(cur_max_uprice_id, 1, 12) as double) cur_max_uprice
		 , cast(substr(cur_max_uprice_id, 13, 12) as signed integer) cur_max_uprice_id
		 , cast(substr(cur_max_price_id, 1, 12) as double) cur_max_price
		 , cast(substr(cur_max_price_id, 13, 12) as signed integer) cur_max_price_id
		 , cast(substr(before_1m_max_uprice_id, 1, 12) as double) before_1m_max_uprice
		 , cast(substr(before_1m_max_uprice_id, 13, 12) as signed integer) before_1m_max_uprice_id
		 , cast(substr(before_1m_max_price_id, 1, 12) as double) before_1m_max_price
		 , cast(substr(before_1m_max_price_id, 13, 12) as signed integer) before_1m_max_price_id
		 , cast(substr(before_1y_max_uprice_id, 1, 12) as double) before_1y_max_uprice
		 , cast(substr(before_1y_max_uprice_id, 13, 12) as signed integer) before_1y_max_uprice_id
		 , cast(substr(before_1y_max_price_id, 1, 12) as double) before_1y_max_price
		 , cast(substr(before_1y_max_price_id, 13, 12) as signed integer) before_1y_max_price_id
	  from (
"""
SELECT_1M_1Y_COMPARE_ARR = ["""
	  	select s.region_key, r.region_name
			 , round(sum(avg_price*count)/sum(count),2) cur_uprice, sum(count) cur_cnt 
			 , max(concat(lpad(max_price,12,'0'),lpad(max_sale_id,12,'0'))) cur_max_uprice_id
""",
"""
	  ) s1, (
	  	select s.region_key
			 , round(sum(avg_price*count)/sum(count), 2) cur_price 
			 , max(concat(lpad(max_price,12,'0'),lpad(max_sale_id,12,'0'))) cur_max_price_id
""",
"""
	  ) s2, (
	    select s.region_key
			 , round(sum(avg_price*count)/sum(count), 2) before_1m_uprice, sum(count) before_1m_cnt 
			 , max(concat(lpad(max_price,12,'0'),lpad(max_sale_id,12,'0'))) before_1m_max_uprice_id
""",
"""
	  ) s3, (
	  	select s.region_key
			 , round(sum(avg_price*count)/sum(count), 2) before_1m_price
			 , max(concat(lpad(max_price,12,'0'),lpad(max_sale_id,12,'0'))) before_1m_max_price_id
""",
"""
	  ) s4, (
	  	select s.region_key
			 , round(sum(avg_price*count)/sum(count), 2) before_1y_uprice, sum(count) before_1y_cnt
			 , max(concat(lpad(max_price,12,'0'),lpad(max_sale_id,12,'0'))) before_1y_max_uprice_id
""",
"""
	  ) s5, (
	  	select s.region_key
			 , round(sum(avg_price*count)/sum(count), 2) before_1y_price
			 , max(concat(lpad(max_price,12,'0'),lpad(max_sale_id,12,'0'))) before_1y_max_price_id
"""]

SELECT_1M_1Y_COMPARE_COMMON_TAIL = """
	  ) s6 
	  where s1.region_key = s2.region_key 
	  	and s2.region_key = s3.region_key 
		and s3.region_key = s4.region_key 
		and s4.region_key = s5.region_key 
		and s5.region_key = s6.region_key
"""

@app.route("/getCompareData")
def getCompareData():

	params = request.args.to_dict()
	ym = params['to_ym']
	danji = params['danji']
	region = params['region_key']

	ymarr = [":ym", ":ym", ":ym_1m", ":ym_1m", ":ym_1y", ":ym_1y"]
	sql = SELECT_1M_1Y_COMPARE_HEAD
	for i, t in enumerate(SELECT_1M_1Y_COMPARE_ARR):
		sql_common = SELECT_1M_1Y_COMPARE_COMMON.replace(":price_gubun", ":price_gubun" + str((i % 2) + 1))
		sql_common = sql_common.replace(":ym", ymarr[i])
		sql = sql + t + sql_common

	sql = sql + SELECT_1M_1Y_COMPARE_COMMON_TAIL
	
	ymd = datetime.strptime(ym+"01", "%Y%m%d")
	ym_1m = (ymd - relativedelta(months=1)).strftime("%Y%m")
	ym_1y = (ymd - relativedelta(years=1)).strftime("%Y%m")

	with app.engine.connect() as connection:
		result = connection.execute(text(sql), ym=ym, ym_1m=ym_1m, ym_1y=ym_1y, \
									price_gubun1='1', price_gubun2='2', danji=danji, region=region)

	rows=result.fetchall()            
	json_data = {}
	json_data['labels'] = []
	for key in result.keys():
		json_data[key] = []

	for res in rows:
		json_data['labels'].append(res['region_name'])
		for key in json_data:
			if key == "labels":
				continue
			json_data[key].append(res[key])

	json_return=json.dumps(json_data)   #string #json
 
	return jsonify(json_return)

