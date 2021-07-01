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
import my_utils

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

JOB_PREFIX = "ITEMS"
app = my_utils.init_app()
logger = my_utils.init_logger(JOB_PREFIX)

API_KEY = app.config['API_KEY']
NUM_OF_ROWS = "1000"

LOAD_INTO_TMP_RAW_DATA = """
	LOAD DATA INFILE %s INTO TABLE tmp_raw_data_new FIELDS TERMINATED BY ',' optionally enclosed by '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS
		(거래금액,@건축년도,년,도로명,도로명건물본번호코드,도로명건물부번호코드,도로명시군구코드,
		도로명일련번호코드,도로명지상지하코드,도로명코드,법정동,법정동본번코드,법정동부번코드,
		법정동시군구코드,법정동읍면동코드,법정동지번코드,아파트,월,일,일련번호,전용면적,지번, 지역코드, 층,해제사유발생일,해제여부, job_key, @vapt_id, ym)
		set apt_id = nullif(@vapt_id, ''), 건축년도 = if(ifnull(@건축년도, '') = '', 1900, @건축년도)
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
		   and job_key = %s
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
		   and job_key = %s
		)
"""

UPDATE_TMP_RAW_MADE_YEAR1 = """
	update tmp_raw_data_new a set 건축년도 = (
		select made_year from apt_master
			where region_key = concat(a.법정동시군구코드, a.법정동읍면동코드)
			  and apt_name = a.아파트
			order by id
			limit 1
		)
		where 건축년도 = 1900
"""

UPDATE_TMP_RAW_MADE_YEAR2 = """
	update tmp_raw_data_new a set 건축년도 = 1970
		where 건축년도 is null
"""

def get_and_load_data(job_key, ym, regions, columns, fname):
	URL = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"
	total_count = 0
	items_list = []
	failed_list = []
	add_columns = { 'job_key': job_key, 'apt_id': None, 'ym': ym }
	for res in regions:
		res = res[:5]
		params = { "LAWD_CD": res, "DEAL_YMD": ym, "serviceKey": API_KEY }
		count = my_utils.get_items(URL, items_list, params, 1, NUM_OF_ROWS, job_key, columns, add_columns)
		if count < 0:
			failed_list.append(res)
		else:
			total_count += count

	finally_failed_list = []
	for res in failed_list:
		params = { "LAWD_CD": res, "DEAL_YMD": ym, "serviceKey": API_KEY }
		count = my_utils.get_items(URL, items_list, params, 1, NUM_OF_ROWS, job_key, columns, add_columns)
		if count >= 0:
			total_count += count
		else:
			finally_failed_list.append(res)
			logger.error("Finally failed get_items : region = " + res)

	items = pd.DataFrame(items_list) 

	items.to_csv(fname, index=False,encoding="utf-8")

	if my_utils.execute_dml(job_key, LOAD_INTO_TMP_RAW_DATA, (fname,)) < 0:
		job_fail(total_count, 0, 0, 0, job_key, ym)

	return total_count


YMs = []
if from_ym == None:
	d = date.today()
	from_ym = "%04d%02d" %(d.year, d.month)
if to_ym == None:
	to_ym = from_ym

logger.info("SaleList Update Start : from_ym = " + from_ym + ", to_ym = " + to_ym)

PID_FILE = app.config['BASE_DIR'] + "/sale_batch.pid"
if os.path.isfile(PID_FILE):
	logger.error("PID file already exists!! " + PID_FILE)
	sys.exit(1)

with open(PID_FILE, "w") as f:
    f.write(str(os.getpid()))

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

sql = "select case when substr(region_key, 1, 5) = '36111' then concat('36110', substr(region_key,6,5)) else region_key end as region_cd \
	from region_info where substr(region_key,3,1) <> '0' and substr(region_key,6,1) = '0'"
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


INSERT_APT_MASTER_NEW = """
	insert into apt_master 
	select null, concat(법정동시군구코드, 법정동읍면동코드), 아파트, 건축년도, null, %s
        from tmp_raw_data2_new
       where (건축년도, concat(법정동시군구코드, 법정동읍면동코드), 아파트)
        not in (select made_year, region_key,  apt_name from apt_master)
	   group by 건축년도, concat(법정동시군구코드, 법정동읍면동코드), 아파트
"""

UPDATE_TMP_RAW_DATA2 = """
	update tmp_raw_data2_new a set apt_id = (
		select id 
		  from apt_master 
		 where made_year = a.건축년도 
		   and region_key = concat(a.법정동시군구코드, a.법정동읍면동코드)
		   and apt_name = a.아파트
		)
	 where apt_id is null
"""

SELECT_APT_ID_NULL = "select * from tmp_raw_data2_new where apt_id is null"

INSERT_APT_SALE_ITEMS = """
	insert into apt_sale_items
		select null, cast(replace(거래금액, ',', '') as signed integer),
	 		   STR_TO_DATE(concat(cast(년 as char), lpad(cast(월 as char), 2, '0'), lpad(cast(일 as char),2,'0')), '%Y%m%d'),
	 		   전용면적, case when 전용면적<=60 then '01' when 전용면적>60 and 전용면적<=85 then '02' when 전용면적>85 and 전용면적<=135 then '03' when 전용면적>135 then '04' end,
	 		   cast(case when 층='' then '0' else 층 end as signed integer), 일련번호, apt_id, concat(cast(년 as char), lpad(cast(월 as char), 2, '0')), %s 
	 	  from tmp_raw_data2_new
"""

INSERT_RAW_DATA = "insert ignore into raw_data_new select * from tmp_raw_data2_new"

DELETE_APT_SALE_ITEMS = "delete from apt_sale_items where id in (select id from apt_sale_deleted where ym = %s and job_key = %s)"

INSERT_APT_SALE_DELETED = """
	insert into apt_sale_deleted
		select a.*, %s 
		  from (
		  	 select a.*
			   from apt_sale_items a, apt_master b
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
				   	   and 아파트 = b.apt_name
				   	   and 일련번호 = a.seq
			   	)
			union
		  	 select a.*
			   from apt_sale_items a, apt_master b
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
				   	   and 아파트 = b.apt_name
				   	   and 일련번호 = a.seq
			   	)
			) a 
"""

for ym in YMs: 
	ym_start_dt = datetime.datetime.now()
	job_key = JOB_PREFIX + "_" + ym_start_dt.strftime('%Y%m%d%H%M%S')
	logger.info(ym + " : starting...")
	my_utils.job_start(JOB_NAME, job_key, ym)

	get_cnt = ins_cnt = del_cnt = apt_cnt = 0 

	my_utils.execute_dml(job_key,"delete from tmp_ym where ym='"+ym+"'")
	my_utils.execute_dml(job_key,"insert into tmp_ym values('"+ym+"')")
	my_utils.execute_dml(job_key, "truncate tmp_raw_data2_new")
#	my_utils.execute_dml(job_key, "truncate tmp_raw_data_error")

	CSV_FILE = os.path.join(app.config['BASE_DIR'], "%s_%s.csv" %(ym, job_key))
	if reuse_tmp_data != True:
		if my_utils.execute_dml(job_key, "truncate tmp_raw_data_new") < 0:
			job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym)

		start_dt = datetime.datetime.now()
		logger.info("get_and_load starting...")
		get_cnt = get_and_load_data(job_key, ym, regions, columns, CSV_FILE);
		logger.info("get_and_load Completed(" + str(datetime.datetime.now() - start_dt) + ") : " + str(get_cnt))

		if my_utils.execute_dml(job_key, "update tmp_raw_data_new set 법정동시군구코드 = '36111' where 법정동시군구코드 = '36110'") < 0:
			job_fail(get_cnt, 0, 0, 0, job_key, ym)
		if my_utils.execute_dml(job_key, "update tmp_raw_data_new set 법정동읍면동코드 = '32000' where 법정동시군구코드 = '41461' and (법정동읍면동코드 = '25931')") < 0:
			job_fail(get_cnt, 0, 0, 0, job_key, ym)
		rows = my_utils.execute_dml(job_key, INSERT_RAW_DATA_ERROR)
		if rows > 0:
			update4 = my_utils.execute_dml(job_key, UPDATE_TMP_RAW_REGION_LEVEL4, (job_key,))
			update5 = my_utils.execute_dml(job_key, UPDATE_TMP_RAW_REGION_LEVEL5, (job_key,))
			if rows !=update4 + update5:
				logger.error("CHCK!!! tmp_raw_data_error Not All Updated : invalid = " + str(rows) + ", updated4 = " + str(update4) + ", updated5 = " + str(update5))
				job_fail(get_cnt, 0, 0, 0, job_key, ym)

	if my_utils.execute_dml(job_key, UPDATE_TMP_RAW_MADE_YEAR1) < 0:
		job_fail(get_cnt, 0, 0, 0, job_key, ym)

	if my_utils.execute_dml(job_key, UPDATE_TMP_RAW_MADE_YEAR2) < 0:
		job_fail(get_cnt, 0, 0, 0, job_key, ym)

	if my_utils.execute_dml(job_key, INSERT_TMP_RAW_DATA2, (ym,)) < 0:
		job_fail(get_cnt, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_MASTER_NEW, (job_key,))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym)
	apt_cnt = rows

	rows = my_utils.execute_dml(job_key, UPDATE_TMP_RAW_DATA2)
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym)

	rows = my_utils.execute_dml(job_key, SELECT_APT_ID_NULL)
	if rows != 0:
		logger.error("CHCK!!! apt_id null exists : " + str(rows))
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_RAW_DATA)
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_ITEMS, (job_key,))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym)
	ins_cnt = rows

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_DELETED, (job_key, ym, ym))
	if rows < 0:
		job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym)
	elif rows > 0:
		rows = my_utils.execute_dml(job_key, DELETE_APT_SALE_ITEMS, (ym, job_key)) 
		if rows < 0:
			job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym)
		del_cnt = rows
	else:
		del_cnt = 0

	my_utils.job_finish(job_key, get_cnt, ins_cnt, del_cnt, apt_cnt)
	ym_end_dt = datetime.datetime.now()

	logger.info(ym + " : Completed(" + str(ym_end_dt - ym_start_dt) + ") : " + str(ins_cnt))
	if os.path.isfile(CSV_FILE):
		os.remove(CSV_FILE)


logger.info("Completed!!")

os.remove(PID_FILE)

