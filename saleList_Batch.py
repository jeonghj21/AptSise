from flask import Flask
from sqlalchemy import create_engine, text
import pandas as pd
import xml.etree.ElementTree as ET
import os
import sys
import requests
import datetime
import time
from datetime import date
import logging

app = Flask(__name__)
app.config.from_envvar('FLASK_CONFIG')

app.engine = create_engine(app.config['DB_URL'], encoding = 'utf-8')

from_ym = None 
to_ym = None
for arg in sys.argv:
	if arg[0:9] == "-from_ym=":
		from_ym = arg[9:]
	elif arg[0:7] == "-to_ym=":
		to_ym = arg[7:]

logger = logging.getLogger()
logger.setLevel(logging.INFO)
fh = logging.FileHandler(filename = app.config['BASE_DIR'] + "/update_"+datetime.datetime.now().strftime('%Y%m%d%H%M%S')+".log")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

API_KEY = app.config['API_KEY']
NUM_OF_ROWS = "1000"
URL = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"

rows = 0

def get_items(item_list, lawdCd, Ymd, pageNo, job_key, columns):
	url = URL + "?LAWD_CD="+lawdCd+"&DEAL_YMD="+Ymd+"&serviceKey="+API_KEY+"&numOfRows="+NUM_OF_ROWS+"&pageNo="+str(pageNo)

	count = 0
	try_count = 0
	content = ""
	success = False
	while not success:
		try:
			response = requests.get(url)
			content = response.content if response.status_code == 200 else ""
			success = True
		except Exception as e:
			try_count = try_count + 1
			if try_count == 5:
				logger.error("Exceeding maximum retries... Exiting...")
				return -1
			else:
				time.sleep(3)


	root = None
	try:
		root = ET.fromstring(content)
	except ET.ParseError as err:
		logger.exception("Response parse error")
		return -1

	header = root.find("header")
	if header == None or header.find("resultCode").text != "00":
		logger.error("Error response :", "header missing" if header == None else header.find("resultMsg").text)
		return -1

	for child in root.find('body').find('items'):
		count = count + 1
		data = { }
		for column in columns:
			element = child.find(column)
			if element == None:
				data[column] = None
			else:
				data[column] = element.text.strip()
		data['job_key'] = job_key 
		data['apt_id'] = None
		data['ym'] = ym
		item_list.append(data)  

	if pageNo == 1:
		totalCount = int(root.find('body').find('totalCount').text)
		totalPage = int(totalCount / int(NUM_OF_ROWS)) + 1
		while pageNo < totalPage:
			count = count + get_items(item_list, lawdCd, Ymd, pageNo + 1, job_key, columns)
			pageNo = pageNo + 1

	return count

INSERT_JOB_ERROR = "insert into job_error values(%s, %s, %s, %s)"


import unicodedata, re
# Get all unicode characters
all_chars = (chr(i) for i in range(sys.maxunicode))
# Get all non printable characters
control_chars = ''.join(c for c in all_chars if unicodedata.category(c) == 'Cc')
# Create regex of above characters
control_char_re = re.compile('[%s]' % re.escape(control_chars))
# Substitute these characters by empty string in the original string.
def remove_control_chars(s):
    return control_char_re.sub('', s)

err_seq = 0

def execute_dml(job_key, sql, params = None, job_log = True):
	sql = remove_control_chars(sql.replace('\t', ' '))
	msg = sql[:30]
	msg = msg[:msg.rfind(' ')] if msg.rfind(' ') > 20 else msg
	logger.info(msg + " Starting...")
	rows = -1
	try:
		connection = app.engine.raw_connection()
		cursor = connection.cursor()
		if params == None:
			cursor.execute(sql)
		else:
			cursor.execute(sql ,params)
		rows = cursor.rowcount
		connection.commit()
		cursor.close()
		logger.info(msg + " Completed : " + str(rows))
	except Exception as e:
		err = "Error : " + msg
		global err_seq
		err_seq = err_seq + 1
		now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
		logger.exception(err)
		if job_log:
			execute_dml(job_key, INSERT_JOB_ERROR, (job_key, err_seq, now, err), False)
	finally:
	    connection.close()

	return rows

UPDATE_JOB_LOG_FAIL = "update job_log set end_dt = str_to_date(%s, '%Y%m%d%H%i%S'), result='N' where job_key = %s"

def job_fail(job_key):
	now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
	execute_dml(job_key, UPDATE_JOB_LOG_FAIL, (now, job_key), False)
	sys.exit(1)

LOAD_INTO_TMP_RAW_DATA = """
	LOAD DATA INFILE %s INTO TABLE tmp_raw_data FIELDS TERMINATED BY ',' optionally enclosed by '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS
		(거래금액,건축년도,년,도로명,도로명건물본번호코드,도로명건물부번호코드,도로명시군구코드,
		도로명일련번호코드,도로명지상지하코드,도로명코드,법정동,법정동본번코드,법정동부번코드,
		법정동시군구코드,법정동읍면동코드,법정동지번코드,아파트,월,일,일련번호,전용면적,지번, 지역코드, 층, job_key, @vapt_id, ym)
		set apt_id = nullif(@vapt_id, '')
"""

INSERT_TMP_RAW_DATA2 = """
	insert into tmp_raw_data2 
		select * from tmp_raw_data 
	 	 where (아파트,건축년도,년,월,일,거래금액,전용면적, 층, 일련번호,법정동시군구코드,법정동읍면동코드,도로명코드,지번) 
	 	   not in (select 아파트,건축년도,년,월,일,거래금액,전용면적, 층, 일련번호,법정동시군구코드,법정동읍면동코드,도로명코드,지번
	 	  from raw_data where ym = %s)
"""

def get_and_load_data(job_key, ym, regions, columns):
	total_count = 0
	items_list = []
	failed_list = []
	for res in regions:
		count = get_items(items_list, res, ym, 1, job_key, columns)
		if count < 0:
			failed_list.append(res)
		else:
			total_count += count

	for res in failed_list:
		count = get_items(items_list, res, ym, 1, job_key, columns)
		if count > 0:
			total_count += count

	items = pd.DataFrame(items_list) 

	fname = os.path.join(app.config['BASE_DIR'], "%s_%s.csv" %(ym, job_key))
	items.to_csv(fname, index=False,encoding="utf-8")

	if execute_dml(job_key, LOAD_INTO_TMP_RAW_DATA, (fname,)) < 0:
		job_fail(job_key)

	if execute_dml(job_key, INSERT_TMP_RAW_DATA2, (ym,)) < 0:
		job_fail(job_key)

	os.remove(fname)

	return total_count


YMs = []
if from_ym == None:
	d = date.today()
	from_ym = "%04d%02d" %(d.year, d.month)
if to_ym == None:
	to_ym = from_ym

logger.info("SaleList Update Start : from_ym = " + from_ym + ", to_ym = " + to_ym)

YMs.append(from_ym)
while from_ym < to_ym:
	year = int(int(from_ym) / 100)
	month = int(from_ym) % 100 + 1
	if month > 12:
		month = month - 12
		year = year + 1
	from_ym = "%04d%02d" %(year, month)
	YMs.append(from_ym)

sql = "select * from ref_region"
with app.engine.connect() as connection:
	result = connection.execute(text(sql))

regions = []
for res in result:
	regions.append(res['region_cd'])

sql = "desc tmp_raw_data"
with app.engine.connect() as connection:
	result = connection.execute(text(sql))

columns = []
for res in result:
	columns.append(res['Field'])

JOB_NAME = "부동산 실거래 정보 현행화"

INSERT_JOB_LOG = "insert into job_log values(%s, %s, %s, str_to_date(%s, '%Y%m%d%H%i%S'), null, null, 0, 0, 0, 0)"

INSERT_APT_MASTER_NEW = """
	insert into apt_master_new 
		select null, 아파트, 법정동시군구코드,법정동읍면동코드,도로명코드,건축년도,'' 
		  from tmp_raw_data2
	 	 where (건축년도,법정동시군구코드,아파트)
	 	   not in (select made_year, region, apt_name from apt_master_new)
	 	 group by 건축년도, 도로명코드,법정동시군구코드,법정동읍면동코드,아파트 
"""

UPDATE_TMP_RAW_DATA2 = """
	update tmp_raw_data2 a set apt_id = (
		select id 
		  from apt_master_new 
		 where made_year = a.건축년도 
		   and road_cd = a.도로명코드 
	 	   and region = a.법정동시군구코드 
		   and dong = a.법정동읍면동코드 
		   and apt_name = a.아파트
		)
"""

INSERT_APT_SALE_NEW = """
	insert into apt_sale_new
		select null, cast(replace(거래금액, ',', '') as signed integer),
	 		   STR_TO_DATE(concat(cast(년 as char), lpad(cast(월 as char), 2, '0'), lpad(cast(일 as char),2,'0')), '%Y%m%d'),
	 		   전용면적, case when 전용면적<=60 then '01' when 전용면적>60 and 전용면적<=85 then '02' when 전용면적>85 and 전용면적<=135 then '03' when 전용면적>135 then '04' end,
	 		   cast(층 as signed integer), 일련번호, apt_id, concat(cast(년 as char), lpad(cast(월 as char), 2, '0')) 
	 	  from tmp_raw_data2
"""

INSERT_RAW_DATA = "insert into raw_data select * from tmp_raw_data2"

DELETE_APT_SALE_NEW = "delete from apt_sale_new where id in (select id from apt_sale_deleted where ym = %s and job_key = %s)"

INSERT_APT_SALE_DELETED = """
	insert into apt_sale_deleted
		select a.*, %s 
		  from apt_sale_new a, apt_master_new b
	 	 where a.apt_id = b.id 
		   and ym = %s
	 	   and (format(price,0), date_format(saled, '%Y%m%d'), area, floor, region, dong,  road_cd, apt_name, seq)
	 	   not in (
		   		select 거래금액, concat(년,lpad(월, 2, '0'), lpad(일, 2, '0')), 전용면적, cast(층 as signed integer),
	 				   법정동시군구코드, 법정동읍면동코드, 도로명코드, 아파트, 일련번호 
				  from raw_data 
				 where ym = %s
			   )
"""

DELETE_APT_MA = "delete from apt_ma_new where ym = %s"
DELETE_APT_REGION_MA = "delete from apt_region_ma_new where ym = %s"

INSERT_APT_SALE_MA = """
	insert into apt_ma_new
		select * from (
			select a.apt_id, b.ym, a.area_type, round(avg(a.price), 2) unit_price, count(*) cnt
			  from tmp_ym b
				 , apt_sale_new a
				 , apt_master_new c
			 where b.ym = %s
			   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
			   and a.apt_id = c.id
			 group by b.ym, a.apt_id, a.area_type
		) a
"""

INSERT_APT_STATS_MA = """
	insert into apt_region_ma_new
		select * 
		  from (
    		select a.region, a.dong, a.danji_flag, b.ym, a.made_year, a.area_type, round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, sum(a.cnt) cnt
	      	  from tmp_ym b, apt_sale_stats_new a
		 	 where b.ym = %s
		   	   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
		 	 group by b.ym, a.region, a.dong, a.danji_flag, a.made_year, a.area_type
		  ) a
"""

INSERT_APT_STATS_MA_REGION = """
	insert into apt_region_ma_new
		select * 
		  from (
    		select a.region, '00000', a.danji_flag, b.ym, a.made_year, a.area_type, round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, sum(a.cnt) cnt
	      	  from tmp_ym b, apt_sale_stats_new a
		 	 where b.ym = %s
			   and region != '11000'
		   	   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
		 	 group by b.ym, a.region, a.danji_flag, a.made_year, a.area_type
		  ) a
"""

DELETE_APT_SALE_STATS_NEW = "delete from apt_sale_stats_new where ym = %s"

INSERT_APT_SALE_STATS_NEW = """
	insert into apt_sale_stats_new
		select region, dong, made_year, area_type, ym, avg(price/(area/3.3)), count(*), 'N'
	 	  from apt_sale_new a, apt_master_new b
	 	 where a.apt_id = b.id and a.ym = %s
	 	 group by region, dong, made_year, area_type, ym
"""

INSERT_APT_SALE_STATS_NEW_Y = """
	insert into apt_sale_stats_new
		select region, dong, made_year, area_type, ym, avg(price/(area/3.3)), count(*), 'Y'
	 	  from apt_sale_new a, apt_master_new b
	 	 where a.apt_id = b.id and a.ym = %s and b.k_apt_id is not null
	 	 group by region, dong, made_year, area_type, ym
"""

INSERT_APT_SALE_STATS_NEW_REGION = """
	insert into apt_sale_stats_new
		select region, '00000', made_year, area_type, ym, (sum(unit_price * cnt) / sum(cnt)), count(*), danji_flag
	 	  from apt_sale_stats_new a
	 	 where a.ym = %s
	 	 group by region, made_year, area_type, ym, danji_flag
"""

INSERT_APT_SALE_STATS_NEW_TOTAL = """
	insert into apt_sale_stats_new
		select '11000', '00000', made_year, area_type, ym, (sum(unit_price * cnt) / sum(cnt)), count(*), danji_flag
	 	  from apt_sale_stats_new a
	 	 where a.ym = %s
		   and dong = '00000'
	 	 group by made_year, area_type, ym, danji_flag
"""

UPDATE_JOB_LOG_SUCCESS = """
	update job_log set 
		end_dt = str_to_date(%s, '%Y%m%d%H%i%S')
	  , result='Y'
	  , get_cnt = %s
	  , insert_cnt = %s
	  , delete_cnt = %s
	  , new_apt_cnt = %s
	 where job_key = %s
"""

for ym in YMs: 
	job_key = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
	logger.info(ym + " : starting...")
	execute_dml(job_key, INSERT_JOB_LOG, (JOB_NAME, job_key, ym, job_key), "insert into job_log")

	execute_dml(job_key,"delete from tmp_ym where ym='"+ym+"'")
	execute_dml(job_key,"insert into tmp_ym values('"+ym+"')")

	if execute_dml(job_key, "truncate tmp_raw_data") < 0 \
	or execute_dml(job_key, "truncate tmp_raw_data2") < 0:
		job_fail(job_key)

	logger.info("get_and_load starting...")
	get_cnt = get_and_load_data(job_key, ym, regions, columns);
	logger.info("get_and_load completed : " + str(get_cnt))

	rows = execute_dml(job_key, INSERT_APT_MASTER_NEW)
	if rows < 0:
		job_fail(job_key)
	apt_cnt = rows


	rows = execute_dml(job_key, UPDATE_TMP_RAW_DATA2)
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, INSERT_RAW_DATA)
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_NEW)
	if rows < 0:
		job_fail(job_key)
	ins_cnt = rows

	rows = execute_dml(job_key, INSERT_APT_SALE_DELETED, (job_key, ym, ym))
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, DELETE_APT_SALE_NEW, (ym, job_key)) 
	if rows < 0:
		job_fail(job_key)
	del_cnt = rows

	rows = execute_dml(job_key, DELETE_APT_SALE_STATS_NEW, (ym,))
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_STATS_NEW, (ym,))
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_STATS_NEW_Y, (ym,))
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_STATS_NEW_TOTAL, (ym,))
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_STATS_NEW_REGION, (ym,))
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, DELETE_APT_MA, (ym,))
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_MA, (ym,))
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, DELETE_APT_REGION_MA, (ym,))
	if rows < 0:
		job_fail(job_key)

	rows = execute_dml(job_key, INSERT_APT_STATS_MA, (ym,))
	if rows < 0:
		job_fail(job_key)

	execute_dml(job_key, UPDATE_JOB_LOG_SUCCESS, (datetime.datetime.now().strftime('%Y%m%d%H%M%S'), get_cnt, ins_cnt, del_cnt, apt_cnt, job_key))

logger.info("Completed!!")
