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
reuse_tmp_data = False
for arg in sys.argv:
	if arg[0:9] == "-from_ym=":
		from_ym = arg[9:]
	elif arg[0:7] == "-to_ym=":
		to_ym = arg[7:]
	elif arg[0:16] == "--reuse_tmp_data":
		reuse_tmp_data = True

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
			item_cnt = get_items(item_list, lawdCd, Ymd, pageNo + 1, job_key, columns)
			if item_cnt < 0:
				return -1
			count = count + item_cnt
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

	start_dt = datetime.datetime.now()
#	logger.info(msg + " Starting...")
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
		logger.info(msg + " Completed(" + str(datetime.datetime.now() - start_dt) + ") : " + str(rows))
	except Exception as e:
		err = "Error : " + sql
		global err_seq
		err_seq = err_seq + 1
		now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
		logger.exception(err)
		if job_log:
			execute_dml(job_key, INSERT_JOB_ERROR, (job_key, err_seq, now, err), False)
	finally:
	    connection.close()

	return rows

UPDATE_JOB_LOG_FAIL = """
	update job_log 
	   set end_dt = str_to_date(%s, '%Y%m%d%H%i%S')
	   	 , result='N'
	  	 , get_cnt = %s
	  	 , insert_cnt = %s
	  	 , delete_cnt = %s
	  	 , new_apt_cnt = %s
	 where job_key = %s
"""

def job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key):
	now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
	execute_dml(job_key, UPDATE_JOB_LOG_FAIL, (now, get_cnt, ins_cnt, del_cnt, apt_cnt, job_key), False)
	sys.exit(1)

LOAD_INTO_TMP_RAW_DATA = """
	LOAD DATA INFILE %s INTO TABLE tmp_raw_data_new FIELDS TERMINATED BY ',' optionally enclosed by '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS
		(거래금액,@건축년도,년,도로명,도로명건물본번호코드,도로명건물부번호코드,도로명시군구코드,
		도로명일련번호코드,도로명지상지하코드,도로명코드,법정동,법정동본번코드,법정동부번코드,
		법정동시군구코드,법정동읍면동코드,법정동지번코드,아파트,월,일,일련번호,전용면적,지번, 지역코드, 층,해제사유발생일,해제여부, job_key, @vapt_id, ym)
		set apt_id = nullif(@vapt_id, ''), 건축년도 = nullif(@건축년도, '')
"""

INSERT_TMP_RAW_DATA2 = """
	insert into tmp_raw_data2_new 
		select * from tmp_raw_data_new 
	 	 where (아파트,건축년도,년,월,일,거래금액,전용면적, 층, 일련번호,법정동시군구코드,법정동읍면동코드,도로명코드,지번) 
	 	   not in (select 아파트,건축년도,년,월,일,거래금액,전용면적, 층, 일련번호,법정동시군구코드,법정동읍면동코드,도로명코드,지번
	 	  			from raw_data_new where ym = %s)
		   and (해제여부 = '' or 해제여부 is null)
"""

INSERT_RAW_DATA_ERROR = """
	insert into tmp_raw_data_error 
		select t.*, r.level 
		  from tmp_raw_data_new t 
		left join region_info r 
		  on concat(법정동시군구코드,법정동읍면동코드) = region_key 
		 where level <> 3
"""

UPDATE_TMP_RAW_REGION_LEVEL4 = """
	update tmp_raw_data_new a 
	  set 법정동읍면동코드 = (
	  	select substr(upper_region, 6) 
		  from region_info 
		 where region_key = concat(a.법정동시군구코드, a.법정동읍면동코드)
		)
	where (법정동시군구코드, 법정동읍면동코드) in (
		select 법정동시군구코드, 법정동읍면동코드 
		  from tmp_raw_data_error
		 where level = 4
		)
"""

UPDATE_TMP_RAW_REGION_LEVEL5 = """
	update tmp_raw_data_new a 
	  set 법정동읍면동코드 = (
	  	select substr(upper_region, 6) 
		  from region_info 
		 where region_key = (
		 	select upper_region 
			  from region_info
			 where region_key = concat(a.법정동시군구코드, a.법정동읍면동코드)
			)
		)
	where (법정동시군구코드, 법정동읍면동코드) in (
		select 법정동시군구코드, 법정동읍면동코드 
		  from tmp_raw_data_error 
		 where level = 5
		)
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
		job_fail(total_count, 0, 0, 0, job_key)

	os.remove(fname)

	return total_count


YMs = []
if from_ym == None:
	d = date.today()
	from_ym = "%04d%02d" %(d.year, d.month)
if to_ym == None:
	to_ym = from_ym

logger.info("SaleList Update Start : from_ym = " + from_ym + ", to_ym = " + to_ym)

YYs = [from_ym[:4]]
YMs.append(from_ym)
while from_ym < to_ym:
	year = int(int(from_ym) / 100)
	month = int(from_ym) % 100 + 1
	if month > 12:
		month = month - 12
		year = year + 1
	from_ym = "%04d%02d" %(year, month)
	YMs.append(from_ym)
	if YYs[len(YYs)-1] != from_ym[:4]:
		YYs.append(from_ym[:4])

sql = "select case when substr(region_key, 1, 5) = '36111' then '36110' else substr(region_key,1,5) end as region_cd from region_info where level=2"
with app.engine.connect() as connection:
	result = connection.execute(text(sql))

regions = []
for res in result:
	regions.append(res['region_cd'])

sql = "desc tmp_raw_data_new"
with app.engine.connect() as connection:
	result = connection.execute(text(sql))

columns = []
for res in result:
	columns.append(res['Field'])

JOB_NAME = "부동산 실거래 정보 현행화"

INSERT_JOB_LOG = "insert into job_log values(%s, %s, %s, str_to_date(%s, '%Y%m%d%H%i%S'), null, null, 0, 0, 0, 0)"

INSERT_APT_MASTER_NEW = """
	insert into apt_master 
	select null, 아파트, concat(법정동시군구코드, 법정동읍면동코드), 도로명코드, 건축년도, null, null
        from tmp_raw_data2_new
       where (건축년도, concat(법정동시군구코드, 법정동읍면동코드), 도로명코드, 아파트)
        not in (select made_year, region_key, road_cd, apt_name from apt_master)
	   group by 건축년도, concat(법정동시군구코드, 법정동읍면동코드), 도로명코드, 아파트
"""

UPDATE_TMP_RAW_DATA2 = """
	update tmp_raw_data2_new a set apt_id = (
		select id 
		  from apt_master 
		 where made_year = a.건축년도 
		   and road_cd = a.도로명코드 
		   and region_key = concat(a.법정동시군구코드, a.법정동읍면동코드)
		   and apt_name = a.아파트
		)
	 where apt_id is null
"""

INSERT_APT_SALE_NEW = """
	insert into apt_sale_new
		select null, cast(replace(거래금액, ',', '') as signed integer),
	 		   STR_TO_DATE(concat(cast(년 as char), lpad(cast(월 as char), 2, '0'), lpad(cast(일 as char),2,'0')), '%Y%m%d'),
	 		   전용면적, case when 전용면적<=60 then '01' when 전용면적>60 and 전용면적<=85 then '02' when 전용면적>85 and 전용면적<=135 then '03' when 전용면적>135 then '04' end,
	 		   cast(case when 층='' then '0' else 층 end as signed integer), 일련번호, apt_id, concat(cast(년 as char), lpad(cast(월 as char), 2, '0')) 
	 	  from tmp_raw_data2_new
"""

INSERT_RAW_DATA = "insert ignore into raw_data_new select * from tmp_raw_data2_new"

DELETE_APT_SALE_NEW = "delete from apt_sale_new where id in (select id from apt_sale_deleted where ym = %s and job_key = %s)"

INSERT_APT_SALE_DELETED = """
	insert into apt_sale_deleted
		select a.*, %s 
		  from (
		  	 select a.*
			   from apt_sale_new a, apt_master b
			  where a.ym = %s 
			    and a.apt_id = b.id
	 	   		and not exists (
		   			select 1
				  	  from tmp_raw_data_new 
				 	 where ym = a.ym
				   	   and ifnull(해제사유발생일, '') = '' 
				   	   and 거래금액 = format(a.price,0)
				   	   and concat(년,lpad(월, 2, '0'), lpad(일, 2, '0')) = date_format(a.saled, '%Y%m%d')
				   	   and 전용면적 = a.area
				   	   and cast(case when 층='' then '0' else 층 end as signed integer) = a.floor
				   	   and concat(법정동시군구코드, 법정동읍면동코드) = b.region_key
				   	   and 도로명코드 = b.road_cd
				   	   and 아파트 = b.apt_name
				   	   and 일련번호 = a.seq
			   	)
			union
		  	 select a.*
			   from apt_sale_new a, apt_master b
			  where a.ym = %s 
			    and a.apt_id = b.id
				and exists (
		   			select 1
				  	  from tmp_raw_data_new 
				 	 where ym = a.ym
				  	   and ifnull(해제사유발생일, '') <> ''
				   	   and 거래금액 = format(a.price,0)
				   	   and concat(년,lpad(월, 2, '0'), lpad(일, 2, '0')) = date_format(a.saled, '%Y%m%d')
				   	   and 전용면적 = a.area
				   	   and cast(case when 층='' then '0' else 층 end as signed integer) = a.floor
				   	   and concat(법정동시군구코드, 법정동읍면동코드) = b.region_key
				   	   and 도로명코드 = b.road_cd
				   	   and 아파트 = b.apt_name
				   	   and 일련번호 = a.seq
			   	)
			) a 
"""

DELETE_APT_MA = "delete from apt_ma_new where ym between %s and date_format(date_add(str_to_date(concat(%s,'01'), '%Y%m%d'), interval 11 month), '%Y%m')"
DELETE_APT_REGION_MA = "delete from apt_region_ma where ym between %s and date_format(date_add(str_to_date(concat(%s,'01'), '%Y%m%d'), interval 11 month), '%Y%m')"

INSERT_APT_MA = """
	insert into apt_ma_new
		select * from (
			select a.apt_id, b.ym, a.area_type, round(avg(a.price/(a.area/3.3)), 2) unit_price, count(*) cnt
			  from tmp_ym b
				 , apt_sale_new a
				 , apt_master c
			 where b.ym between %s and date_format(date_add(str_to_date(concat(%s,'01'), '%Y%m%d'), interval 11 month), '%Y%m')
			   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
			   and a.apt_id = c.id
			 group by b.ym, a.apt_id, a.area_type
		) a
"""

INSERT_APT_REGION_MA = """
	insert into apt_region_ma
		select * 
		  from (
    		select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type, round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, sum(a.cnt) cnt
	      	  from tmp_ym b, apt_sale_stats a
		 	 where b.ym between %s and date_format(date_add(str_to_date(concat(%s,'01'), '%Y%m%d'), interval 11 month), '%Y%m')
		   	   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
		 	 group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
		  ) a
"""

DELETE_APT_SALE_STATS_NEW = "delete from apt_sale_stats where ym = %s"

INSERT_APT_SALE_STATS_ALL = """
	insert into apt_sale_stats
		select region_key, 3, made_year, area_type, ym, 'N', avg(price/(area/3.3)), count(*)
	 	  from apt_sale_new a, apt_master b
	 	 where a.apt_id = b.id and a.ym = %s
	 	 group by region_key, made_year, area_type, ym
"""

INSERT_APT_SALE_STATS_DANJI_Y = """
	insert into apt_sale_stats
		select region_key, 3, made_year, area_type, ym, 'Y', avg(price/(area/3.3)), count(*)
	 	  from apt_sale_new a, apt_master b
	 	 where a.apt_id = b.id and a.ym = %s and b.k_apt_id is not null
	 	 group by region_key, made_year, area_type, ym
"""

INSERT_APT_SALE_STATS_LEVEL_2 = """
	insert into apt_sale_stats
		select r.upper_region, 2, made_year, area_type, ym, danji_flag, (sum(unit_price * cnt) / sum(cnt)), sum(cnt)
	 	  from apt_sale_stats a, region_info r
	 	 where a.ym = %s
		   and a.region_key = r.region_key
		   and a.level = 3
	 	 group by r.upper_region, made_year, area_type, ym, danji_flag
"""

INSERT_APT_SALE_STATS_LEVEL_1 = """
	insert into apt_sale_stats
		select r.upper_region, 1, made_year, area_type, ym, danji_flag, (sum(unit_price * cnt) / sum(cnt)), sum(cnt)
	 	  from apt_sale_stats a, region_info r
	 	 where a.ym = %s
		   and a.region_key = r.region_key
		   and a.level = 2
	 	 group by r.upper_region, made_year, area_type, ym, danji_flag
"""

INSERT_APT_SALE_STATS_LEVEL_0 = """
	insert into apt_sale_stats
		select '0000000000', 0, made_year, area_type, ym, danji_flag, (sum(unit_price * cnt) / sum(cnt)), sum(cnt)
	 	  from apt_sale_stats a
	 	 where a.ym = %s
		   and a.level = 1
	 	 group by made_year, area_type, ym, danji_flag
"""

INSERT_QBOX_STATS_1 = """
insert into apt_qbox_stats   
		select region_key, level, '1', danji, ym
			 , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
			 , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
			 , 1q_price, 3q_price, med_price, avg_price
		  from (             
		  	select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
				 , max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id
				 , min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id          
			 	 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
			 	 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
			 	 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
				 , round(avg(price), 2) avg_price
"""

QBOX_FROMs = [" from region_info r, apt_sale_new a, apt_master m"
			 , " from region_info r, region_info r1, apt_sale_new a, apt_master m" 
			 , " from region_info r, region_info r1, region_info r2, apt_sale_new a, apt_master m"
			 , " from (select '0000000000' region_key, 0 level from dual) r, apt_sale_new a, apt_master m"]

QBOX_WHEREs = [
"""
		 where r.level = 3 
		   and r.region_key = m.region_key
		   and m.id = a.apt_id
		   and a.ym = %s
"""
,
"""
		 where r.level = 2
		   and r.region_key = r1.upper_region           
		   and r1.region_key = m.region_key                
		   and m.id = a.apt_id             
		   and a.ym = %s
""" 
, 
"""
		 where r.level = 1
		   and r.region_key = r1.upper_region           
		   and r1.region_key = r2.upper_region           
		   and r2.region_key = m.region_key                
		   and m.id = a.apt_id
		   and a.ym = %s
"""
, 
"""
		 where m.id = a.apt_id
		   and a.ym = %s
"""
]

INSERT_QBOX_STATS_2 = """
insert into apt_qbox_stats   
		select region_key, level, '2', danji, ym
			 , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
			 , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
			 , 1q_price, 3q_price, med_price, avg_price
		  from (             
		  	select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
				 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
				 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id          
			 	 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
			 	 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
			 	 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
				 , round(avg(price), 2) avg_price
"""

QBOX_END = " group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a"

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

def update_stats(job_key, ym, get_cnt, ins_cnt, del_cnt, apt_cnt):

	rows = execute_dml(job_key, DELETE_APT_SALE_STATS_NEW, (ym,))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_STATS_ALL, (ym,))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_STATS_DANJI_Y, (ym,))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_STATS_LEVEL_2, (ym,))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_STATS_LEVEL_1, (ym,))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_STATS_LEVEL_0, (ym,))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, DELETE_APT_MA, (ym, ym))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, INSERT_APT_MA, (ym, ym))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, DELETE_APT_REGION_MA, (ym, ym))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, INSERT_APT_REGION_MA, (ym, ym))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)


def update_qbox_stats(job_key, ym, get_cnt, ins_cnt, del_cnt, apt_cnt):

	rows = execute_dml(job_key, "delete from apt_qbox_stats where ym = %s", (ym,))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	execute_dml(job_key, "SET GROUP_CONCAT_MAX_LEN = 10485760")
	for i in range(0, 4):
		sql = INSERT_QBOX_STATS_1 + QBOX_FROMs[i] + QBOX_WHEREs[i] + QBOX_END
		rows = execute_dml(job_key, sql, (ym,))
		if rows < 0:
			job_fail(0, 0, del_cnt, 0, job_key)

	for i in range(0, 4):
		sql = INSERT_QBOX_STATS_2 + QBOX_FROMs[i] + QBOX_WHEREs[i] + QBOX_END
		rows = execute_dml(job_key, sql, (ym,))
		if rows < 0:
			job_fail(0, 0, del_cnt, 0, job_key)

for ym in YMs: 
	ym_start_dt = datetime.datetime.now()
	job_key = ym_start_dt.strftime('%Y%m%d%H%M%S')
	logger.info(ym + " : starting...")
	execute_dml(job_key, INSERT_JOB_LOG, (JOB_NAME, job_key, ym, job_key), "insert into job_log")

	get_cnt = ins_cnt = del_cnt = apt_cnt = 0 

	execute_dml(job_key,"delete from tmp_ym where ym='"+ym+"'")
	execute_dml(job_key,"insert into tmp_ym values('"+ym+"')")
	execute_dml(job_key, "truncate tmp_raw_data2_new")

	if reuse_tmp_data != True:
		if execute_dml(job_key, "truncate tmp_raw_data_new") < 0:
			job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

		start_dt = datetime.datetime.now()
		logger.info("get_and_load starting...")
		get_cnt = get_and_load_data(job_key, ym, regions, columns);
		logger.info("get_and_load Completed(" + str(datetime.datetime.now() - start_dt) + ") : " + str(get_cnt))

		logger.info("update invalid region cod : " + str(datetime.datetime.now() - start_dt) + ") : " + str(get_cnt))

		if execute_dml(job_key, "update tmp_raw_data_new set 법정동시군구코드 = '36111' where 법정동시군구코드 = '36110'") < 0:
			job_fail(get_cnt, 0, 0, 0, job_key)
		if execute_dml(job_key, "update tmp_raw_data_new set 법정동읍면동코드 = '32000' where 법정동시군구코드 = '41461' and (법정동읍면동코드 = '25931')") < 0:
			job_fail(get_cnt, 0, 0, 0, job_key)
		rows = execute_dml(job_key, INSERT_RAW_DATA_ERROR)
		if rows > 0:
			update4 = execute_dml(job_key, UPDATE_TMP_RAW_REGION_LEVEL4)
			update5 = execute_dml(job_key, UPDATE_TMP_RAW_REGION_LEVEL5)
			if rows > update4 + update5:
				logger.error("CHCK!!! tmp_raw_data_error Not All Updated : invalid = " + str(rows) + ", updated = " + str(update4 + update5))
				job_fail(get_cnt, 0, 0, 0, job_key)

	if execute_dml(job_key, INSERT_TMP_RAW_DATA2, (ym,)) < 0:
		job_fail(get_cnt, 0, 0, 0, job_key)


	rows = execute_dml(job_key, INSERT_APT_MASTER_NEW)
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)
	apt_cnt = rows

	rows = execute_dml(job_key, UPDATE_TMP_RAW_DATA2)
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, INSERT_RAW_DATA)
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, INSERT_APT_SALE_NEW)
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)
	ins_cnt = rows

	rows = execute_dml(job_key, INSERT_APT_SALE_DELETED, (job_key, ym, ym))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)

	rows = execute_dml(job_key, DELETE_APT_SALE_NEW, (ym, job_key)) 
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key)
	del_cnt = rows

	if ins_cnt > 0 or del_cnt > 0:
		update_stats(job_key, ym, get_cnt, ins_cnt, del_cnt, apt_cnt)
		update_qbox_stats(job_key, ym, get_cnt, ins_cnt, del_cnt, apt_cnt)

	ym_end_dt = datetime.datetime.now()
	execute_dml(job_key, UPDATE_JOB_LOG_SUCCESS, (ym_end_dt.strftime('%Y%m%d%H%M%S'), get_cnt, ins_cnt, del_cnt, apt_cnt, job_key))

	logger.info(ym + " : Completed(" + str(ym_end_dt - ym_start_dt) + ") : " + str(ins_cnt))

logger.info("Completed!!")

