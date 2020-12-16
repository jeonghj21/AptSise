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
fh = logging.FileHandler(filename = "/home/jeonghj21/aptsise/update_"+datetime.datetime.now().strftime('%Y%m%d%H%M%S')+".log")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

API_KEY = "kowxleiR8vdBv9Du%2BV5P%2BiRZkzWVDZZi9P3BzCA8etSREsXh991q8cu4AhU1dsAFxe3btGhEA1%2FupLgRLn1iQw%3D%3D"
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
		item_list.append(data)  

	if pageNo == 1:
		totalCount = int(root.find('body').find('totalCount').text)
		totalPage = int(totalCount / int(NUM_OF_ROWS)) + 1
		while pageNo < totalPage:
			count = count + get_items(item_list, lawdCd, Ymd, pageNo + 1, job_key, columns)
			pageNo = pageNo + 1

	return count

err_seq = 0
def execute_dml(job_key, sql, params, msg, job_log = True):
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
	except Exception as e:
		err = "Error : " + msg
		err_seq = err_seq + 1
		now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
		logger.exception(err)
		if job_log:
			execute_dml("insert into job_error values(%s, %s, %s, %s)", (job_key, err_seq, now, err), "insert into job_error", False)
	finally:
	    connection.close()

	return rows

def job_fail(job_key):
	now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
	execute_dml(job_key, "update job_log set end_dt = str_to_date(%s, '%Y%m%d%H%i%S'), result='Y' where job_key = %s", (now, job_key), "update job_log set fail", False)
	sys.exit()

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

	fname = os.path.join("/home/jeonghj21/aptsise", "%s_%s.csv" %(ym, job_key))
	items.to_csv(fname, index=False,encoding="utf-8")

	sql = "LOAD DATA INFILE %s INTO TABLE tmp_raw_data FIELDS TERMINATED BY ',' optionally enclosed by '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS"
	sql += " (거래금액,건축년도,년,도로명,도로명건물본번호코드,도로명건물부번호코드,도로명시군구코드,"
	sql += "도로명일련번호코드,도로명지상지하코드,도로명코드,법정동,법정동본번코드,법정동부번코드,"
	sql += "법정동시군구코드,법정동읍면동코드,법정동지번코드,아파트,월,일,일련번호,전용면적,지번, 지역코드, 층, job_key, @vapt_id, ym)"
	sql += " set apt_id = nullif(@vapt_id, '')"
	if execute_dml(job_key, sql, (fname,), "LOAD DATA " + fname) < 0:
		job_fail(job_key)

	logger.info("LOAD DATA INFILE '" + fname + "' INTO TABLE tmp_raw_data completed : " + str(len(items_list)))

	sql = "insert into tmp_raw_data2 select * from tmp_raw_data "
	sql += " where (아파트,건축년도,년,월,일,거래금액,전용면적, 층, 일련번호,법정동시군구코드,법정동읍면동코드,도로명코드,지번) "
	sql += " not in (select 아파트,건축년도,년,월,일,거래금액,전용면적, 층, 일련번호,법정동시군구코드,법정동읍면동코드,도로명코드,지번"
	sql += " from raw_data where ym = %s)"
	if execute_dml(job_key, sql, (ym,), "insert into tmp_raw_data2") < 0:
		job_fail(job_key)

	os.remove(fname)

	return total_count


YMs = []
if from_ym == None:
	d = date.today()
	from_ym = "%04d%02d" %(d.year, d.month)
if to_ym == None:
	to_ym = from_ym

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

for ym in YMs: 
	job_key = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
	logger.info(ym + " : starting...")
	execute_dml(job_key, "insert into job_log values(%s, %s, %s, str_to_date(%s, '%Y%m%d%H%i%S'), null, 0, null)", (JOB_NAME, job_key, ym, job_key), "insert into job_log")

	if execute_dml(job_key, "truncate tmp_raw_data", None, "truncate tmp_raw_data") < 0 or execute_dml(job_key, "truncate tmp_raw_data2", None, "truncate tmp_raw_data2") < 0:
		job_fail(job_key)

	logger.info("get_and_load starting...")
	count = get_and_load_data(job_key, ym, regions, columns);
	logger.info("get_and_load completed : " + str(count))

	logger.info("inserting into apt_master_new...")
	sql = "insert into apt_master_new "
	sql += "  select null, 아파트, 법정동시군구코드,법정동읍면동코드,도로명코드,건축년도 from tmp_raw_data2 "
	sql += " where (건축년도,도로명코드,법정동시군구코드,법정동읍면동코드,아파트)"
	sql += " not  in (select made_year, road_cd, region, dong, apt_name from apt_master_new)" 
	sql += " group by 건축년도, 도로명코드,법정동시군구코드,법정동읍면동코드,아파트" 
	if execute_dml(job_key, sql, None, "insert into apt_master_new") < 0:
		job_fail(job_key)

	logger.info("insert into apt_master_new completed : " + str(rows))

	logger.info("update tmp_raw_data2 set apt_id Starting...")
	sql = "update tmp_raw_data2 a set apt_id = "
	sql += " (select id from apt_master_new where made_year = a.건축년도 and road_cd=a.도로명코드 "
	sql += " and region=a.법정동시군구코드 and dong=a.법정동읍면동코드 and apt_name=a.아파트)"
	if execute_dml(job_key, sql, None, "update tmp_raw_data2 set apt_id") < 0:
		job_fail(job_key)

	logger.info("update tmp_raw_data2 set apt_id completed : " + str(rows))
 
	logger.info("insert into raw_data Starting...")
	if execute_dml(job_key, "insert into raw_data select * from tmp_raw_data2", None, "insert into raw_data") < 0:
		job_fail(job_key)
	logger.info("insert into raw_data completed : " + str(rows))

	logger.info("insert into apt_sale_new Starting...")
	sql = "insert into apt_sale_new"
	sql += " select null, cast(replace(거래금액, ',', '') as signed integer),"
	sql += " STR_TO_DATE(concat(cast(년 as char), lpad(cast(월 as char), 2, '0'), lpad(cast(일 as char),2,'0')), '%Y%m%d'),"
	sql += " 전용면적, case when 전용면적<=60 then '01' when 전용면적>60 and 전용면적<=85 then '02' when 전용면적>85 and 전용면적<=135 then '03' when 전용면적>135 then '04' end,"
	sql += " cast(층 as signed integer), apt_id, concat(cast(년 as char), lpad(cast(월 as char), 2, '0')) "
	sql += " from tmp_raw_data2"
	if execute_dml(job_key, sql, None, "insert into apt_sale_new") < 0:
		job_fail(job_key)

	logger.info("insert into apt_sale_new completed : " + str(rows))


for ym in YMs: 
	logger.info("Starting stats for " + ym)

	if execute_dml(job_key, "delete from apt_sale_stats_new where ym = %s", (ym,), "delete from apt_sale_stats_new") < 0:
		job_fail(job_key)

	logger.info("delete from apt_sale_stats_new completed : " + str(rows))

	logger.info("insert into apt_sale_stats_new Starting...")
	sql = "insert into apt_sale_stats_new"
	sql += " select region, dong, made_year, area_type, ym, avg(price/(area/3.3)), count(*), 'N'"
	sql += " from apt_sale_new a, apt_master_new b"
	sql += " where a.apt_id = b.id and a.ym = %s"
	sql += " group by region, dong, made_year, area_type, ym"
	if execute_dml(job_key, sql, (ym,), "insert into apt_sale_stats_new") < 0:
		job_fail(job_key)
	logger.info("insert into apt_sale_stats_new completed : " + str(rows))

	execute_dml(job_key, "update job_log set end_dt = str_to_date(%s, '%Y%m%d%H%i%S'), process_cnt = %s, result='Y' where job_key = %s", (datetime.datetime.now().strftime('%Y%m%d%H%M%S'), rows, job_key), "update job_log set end_dt")

logger.info("Completed!!")
