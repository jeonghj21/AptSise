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
stats_only = False
qbox_only = False
for arg in sys.argv:
	arg = arg.split("=", 1)
	if arg[0][1:] == "from_ym":
		from_ym = arg[1]
	elif arg[0][1:] == "to_ym":
		to_ym = arg[1]
	elif arg[0][1:] == "reuse_tmp_data":
		reuse_tmp_data = True
	elif arg[0][1:] == "stats_only":
		stats_only = True
	elif arg[0][1:] == "qbox_only":
		qbox_only = True

JOB_PREFIX = "SALES"

app = my_utils.init_app()
logger = my_utils.init_logger(JOB_PREFIX)

PID_FILE = app.config['BASE_DIR'] + "/sales_update_batch.pid"
API_KEY = app.config['API_KEY']
NUM_OF_ROWS = "1000"

YMs = []
if from_ym == None:
	d = date.today()
	from_ym = "%04d%02d" %(d.year, d.month)
if to_ym == None:
	to_ym = from_ym

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

logger.info("Sales Update Batch Start : from_ym = " + YMs[0] + ", to_ym = " + to_ym)

LOAD_INTO_TMP_RAW_DATA = """
	LOAD DATA INFILE %s INTO TABLE tmp_raw_data_new FIELDS TERMINATED BY ',' optionally enclosed by '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS
		(거래금액,@건축년도,년,도로명,도로명건물본번호코드,도로명건물부번호코드,도로명시군구코드,
		도로명일련번호코드,도로명지상지하코드,도로명코드,법정동,법정동본번코드,법정동부번코드,
		법정동시군구코드,법정동읍면동코드,법정동지번코드,아파트,월,일,일련번호,전용면적,지번, 지역코드, 층,해제사유발생일,해제여부, job_key, @vapt_id, ym)
		set apt_id = nullif(@vapt_id, ''), 건축년도 = if(ifnull(@건축년도, '') = '', 1900, @건축년도)
"""

INSERT_TMP_RAW_DATA2 = """
	insert into tmp_raw_data2_new 
		select * from (
			select
				  거래금액,건축년도,년,도로명,도로명건물본번호코드,도로명건물부번호코드,도로명시군구코드
				, 도로명일련번호코드,도로명지상지하코드,도로명코드,법정동,법정동본번코드,법정동부번코드
				, 법정동시군구코드,법정동읍면동코드,법정동지번코드
				, case when substr(아파트,1,2) = substr(r.region_name,1,2) and char_length(아파트) > 3 then substr(아파트,3) else 아파트 end 아파트
				, 월,일,일련번호,전용면적,지번, 지역코드, 층,해제사유발생일,해제여부, job_key, apt_id, ym
			  from tmp_raw_data_new a, region_info r
			 where concat(법정동시군구코드,법정동읍면동코드) = r.region_key
			) a
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

def items_job_fail( job_key, ym, get_cnt=0, apt_cnt=0, ins_cnt=0, del_cnt=0):
	os.remove(PID_FILE)
	my_utils.job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym, \
				not (stats_only or qbox_only), not qbox_only)
	logger.info(ym + " : Failed")

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
		items_job_fail(job_key, ym, total_count)

	return total_count

JOB_NAME = "부동산 실거래 통계 생성"

DELETE_APT_MA = "delete from apt_ma_new where ym between %s and date_format(date_add(str_to_date(concat(%s,'01'), '%Y%m%d'), interval 11 month), '%Y%m')"
DELETE_APT_REGION_MA = "delete from apt_region_ma where ym between %s and date_format(date_add(str_to_date(concat(%s,'01'), '%Y%m%d'), interval 11 month), '%Y%m')"

INSERT_APT_MA = """
	insert into apt_ma_new
		select * from (
			select a.apt_id, b.ym, a.area_type
				 , round(avg(a.price/(a.area/3.3)), 2) unit_price, round(avg(a.price), 2) price, count(*) cnt
			  from tmp_ym b
				 , apt_sale_items a
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
    		select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type
				 , round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, round(sum(a.price*a.cnt)/sum(a.cnt), 2) price, sum(a.cnt) cnt
	      	  from tmp_ym b, apt_sale_stats a
		 	 where b.ym between %s and date_format(date_add(str_to_date(concat(%s,'01'), '%Y%m%d'), interval 11 month), '%Y%m')
		   	   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
		 	 group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
		  ) a
"""

DELETE_APT_SALE_STATS_NEW = "delete from apt_sale_stats where ym = %s"

INSERT_APT_SALE_STATS_ALL = """
	insert into apt_sale_stats
		select region_key, 3, made_year, area_type, a.ym, 'N'
			 , round(avg(price/(area/3.3)), 2) unit_price, round(avg(price), 2) price, count(*)
	 	  from apt_sale_items a, apt_master b
	 	 where a.apt_id = b.id and a.ym = %s
	 	 group by region_key, made_year, area_type, a.ym
"""

INSERT_APT_SALE_STATS_DANJI_Y = """
	insert into apt_sale_stats
		select region_key, 3, made_year, area_type, a.ym, 'Y'
			 , round(avg(price/(area/3.3)), 2) unit_price, round(avg(price), 2) price, count(*)
	 	  from apt_sale_items a, apt_master b
	 	 where a.apt_id = b.id and a.ym = %s and ifnull(b.danji_flag, 'N') = 'Y'
	 	 group by region_key, made_year, area_type, a.ym
"""

INSERT_APT_SALE_STATS_LEVEL_2 = """
	insert into apt_sale_stats
		select r.upper_region, 2, made_year, area_type, ym, danji_flag
			 , round((sum(unit_price * cnt) / sum(cnt)), 2), round((sum(price * cnt) / sum(cnt)), 2), sum(cnt)
	 	  from apt_sale_stats a, region_info r
	 	 where a.ym = %s
		   and a.region_key = r.region_key
		   and a.level = 3
	 	 group by r.upper_region, made_year, area_type, ym, danji_flag
"""

INSERT_APT_SALE_STATS_LEVEL_1 = """
	insert into apt_sale_stats
		select r.upper_region, 1, made_year, area_type, ym, danji_flag
			 , round((sum(unit_price * cnt) / sum(cnt)), 2), round((sum(price * cnt) / sum(cnt)), 2), sum(cnt)
	 	  from apt_sale_stats a, region_info r
	 	 where a.ym = %s
		   and a.region_key = r.region_key
		   and a.level = 2
	 	 group by r.upper_region, made_year, area_type, ym, danji_flag
"""

INSERT_APT_SALE_STATS_LEVEL_0 = """
	insert into apt_sale_stats
		select '0000000000', 0, made_year, area_type, ym, danji_flag
			 , round((sum(unit_price * cnt) / sum(cnt)), 2)
			 , round((sum(price * cnt) / sum(cnt)), 2)
			 , sum(cnt)
	 	  from apt_sale_stats a
	 	 where a.ym = %s
		   and a.level = 1
	 	 group by made_year, area_type, ym, danji_flag
"""

INSERT_QBOX_STATS_COMMON = """
insert into apt_qbox_stats   
		select region_key, level, %s, %s, ym
			 , cast(substr(max_price_id, 1, 12) as double)
			 , cast(substr(max_price_id, 13, 12) as signed integer)
			 , cast(substr(min_price_id, 1, 12) as double)
			 , cast(substr(min_price_id, 13, 12) as signed integer)
			 , 1q_price, 3q_price, med_price, avg_price
			 , count
"""

INSERT_QBOX_STATS = [
"""
		  from (             
		  	select a.ym, r.region_key, r.level
				 , max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id
				 , min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id          
			 	 , substring_index(
				 		substring_index(
				 			group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ',')
									, ',', (((50 / 100) * count(0)) + 1))
						,',',-1) as med_price
			 	 , substring_index(
				 		substring_index(
							group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ',')
									, ',', (((25 / 100) * count(0)) + 1))
							,',',-1) as 1q_price
			 	 , substring_index(
				 		substring_index(
							group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ',')
									, ',', (((75 / 100) * count(0)) + 1))
							,',',-1) as 3q_price
				 , round(avg(price/(area/3.3)), 2) avg_price
				 , count(0) count
""",
"""
		  from (             
		  	select a.ym, r.region_key, r.level
				 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
				 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id          
			 	 , substring_index(
				 		substring_index(
							group_concat(price order by price asc separator ',')
							, ',', (((50 / 100) * count(0)) + 1))
						,',',-1) as med_price
			 	 , substring_index(
				 		substring_index(
							group_concat(price order by price asc separator ',')
							, ',', (((25 / 100) * count(0)) + 1))
						,',',-1) as 1q_price
			 	 , substring_index(
				 		substring_index(
							group_concat(price order by price asc separator ',')
								, ',', (((75 / 100) * count(0)) + 1))
						,',',-1) as 3q_price
				 , round(avg(price), 2) avg_price
				 , count(0) count
"""
]

QBOX_FROMs = [
	" from region_info r, apt_sale_items a, apt_master m", 
	" from region_info r, region_info r1, apt_sale_items a, apt_master m" , 
	" from region_info r, region_info r1, region_info r2, apt_sale_items a, apt_master m", 
	" from (select '0000000000' region_key, 0 level from dual) r, apt_sale_items a, apt_master m"
]

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

SELECT_QBOX_AREA_AND_YEAR_ALL = "			 , '00', 0"
SELECT_QBOX_AREA_AND_YEAR = "			 , area_type, made_year"

QBOX_END = " group by a.ym, r.region_key, r.level, a.area_type, m.made_year) a"
QBOX_END_ALL = " group by a.ym, r.region_key, r.level) a"


def update_stats(job_key, ym):

	rows = my_utils.execute_dml(job_key, DELETE_APT_SALE_STATS_NEW, (ym,))
	if rows < 0:
		items_job_fail(job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_ALL, (ym,))
	if rows < 0:
		items_job_fail(job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_DANJI_Y, (ym,))
	if rows < 0:
		items_job_fail(job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_LEVEL_2, (ym,))
	if rows < 0:
		items_job_fail(job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_LEVEL_1, (ym,))
	if rows < 0:
		items_job_fail(job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_LEVEL_0, (ym,))
	if rows < 0:
		items_job_fail(job_key, ym)

	rows = my_utils.execute_dml(job_key, DELETE_APT_MA, (ym, ym))
	if rows < 0:
		items_job_fail(job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_MA, (ym, ym))
	if rows < 0:
		items_job_fail(job_key, ym)

	rows = my_utils.execute_dml(job_key, DELETE_APT_REGION_MA, (ym, ym))
	if rows < 0:
		items_job_fail(job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_REGION_MA, (ym, ym))
	if rows < 0:
		items_job_fail(job_key, ym)

def save_qbox_stat(add_select, add_where, group_by, ym, danji):
	for i in range(0, 4):
		for j in range(1, 3):
			sql = INSERT_QBOX_STATS_COMMON + add_select \
				+ INSERT_QBOX_STATS[j-1] + add_select + QBOX_FROMs[i] \
				+ QBOX_WHEREs[i] + add_where + group_by
			my_utils.execute_dml(job_key, "SET GROUP_CONCAT_MAX_LEN = 4294967295")
			rows = my_utils.execute_dml(job_key, sql, (str(j), danji, ym))
			if rows < 0:
				items_job_fail(job_key, ym)

def update_qbox_stats(job_key, ym):

	rows = my_utils.execute_dml(job_key, "delete from apt_qbox_stats where ym = %s", (ym,))
	if rows < 0:
		items_job_fail(job_key, ym)

#	for each area_type, made_year
	save_qbox_stat(SELECT_QBOX_AREA_AND_YEAR, "", QBOX_END, ym, 'N')
	save_qbox_stat(SELECT_QBOX_AREA_AND_YEAR, " and m.danji_flag='Y'", QBOX_END, ym, 'Y')

#	for all area_type, made_year
	save_qbox_stat(SELECT_QBOX_AREA_AND_YEAR_ALL, "", QBOX_END_ALL, ym, 'N')
	save_qbox_stat(SELECT_QBOX_AREA_AND_YEAR_ALL, " and m.danji_flag='Y'", QBOX_END_ALL, ym, 'Y')

if os.path.isfile(PID_FILE):
	logger.error("PID file already exists!! " + PID_FILE)
	sys.exit(1)

with open(PID_FILE, "w") as f:
    f.write(str(os.getpid()))

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
	select null, a.region_key, apt_name, a.made_year, job_key, a.jibun1, a.jibun2
		 , case when ifnull(n.family_cnt, 0) >= 300 then 'Y' else 'N' end, n.id 
		from 
			(select concat(법정동시군구코드, 법정동읍면동코드) region_key, min(아파트) apt_name
				   , 건축년도 made_year, %s job_key, 법정동본번코드 jibun1, 법정동부번코드 jibun2
				   , (select id from naver_complex_info n
				       where region_key = concat(t.법정동시군구코드, t.법정동읍면동코드)
		  				 and jibun1
		    			   = (case when t.법정동본번코드 regexp('^[0-9]+$') 
						   				then cast(t.법정동본번코드 as signed int)
		  		  				   else replace(t.법정동본번코드, '산', '') end)
		  				 and jibun2
		    			   = (case when t.법정동부번코드 regexp('^[0-9]+$') 
						   				then cast(t.법정동부번코드 as signed int)
		  		  				   else replace(t.법정동부번코드, '산', '') end)
					   order by t.건축년도 - made_year, category
					   limit 1) naver_id
				from tmp_raw_data2_new t      
			   where (건축년도, concat(법정동시군구코드, 법정동읍면동코드), 법정동본번코드, 법정동부번코드)
			   		not in (select made_year, region_key,  jibun1, jibun2 from apt_master)
 			   group by 건축년도, 법정동시군구코드, 법정동읍면동코드, 법정동본번코드, 법정동부번코드
			 ) a 
			 left join naver_complex_info n
			 	on    ifnull(a.naver_id, 0) = n.id
"""

UPDATE_TMP_RAW_DATA2 = """
	update tmp_raw_data2_new a set apt_id = (
		select id 
		  from apt_master 
		 where made_year = a.건축년도 
		   and region_key = concat(a.법정동시군구코드, a.법정동읍면동코드)
		   and jibun1 = 법정동본번코드
		   and jibun2 = 법정동부번코드
		)
	 where apt_id is null
"""

SELECT_APT_ID_NULL = "select * from tmp_raw_data2_new where apt_id is null"

UPDATE_REGION3_APT_YN = """
	update region_info r set apt_yn = 'Y'
	  where region_key in (select concat(법정동시군구코드, 법정동읍면동코드) from tmp_raw_data2_new )
"""

UPDATE_REGION2_APT_YN = """
	update region_info r set apt_yn = 'Y'       
	 where exists (
	 	select 1 from (select * from region_info where upper_region=r.region_key and apt_yn = 'Y') a)
"""

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
					   and 법정동본번코드 = b.jibun1 
					   and 법정동부번코드 = b.jibun2
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
					   and 법정동본번코드 = b.jibun1 
					   and 법정동부번코드 = b.jibun2
				   	   and 일련번호 = a.seq
			   	)
			) a 
"""

def save_tmp_raw_data(job_key, ym):

	if my_utils.execute_dml(job_key, "truncate tmp_raw_data_new") < 0:
		items_job_fail(job_key, ym)

	start_dt = datetime.datetime.now()
	logger.info("get_and_load starting...")

	CSV_FILE = os.path.join(app.config['BASE_DIR'], "%s_%s.csv" %(ym, job_key))
	counts[GET_CNT] = get_and_load_data(job_key, ym, regions, columns, CSV_FILE);
	logger.info("get_and_load Completed(" + str(datetime.datetime.now() - start_dt) + ") : " + str(counts[GET_CNT]))

	os.remove(CSV_FILE)

	if my_utils.execute_dml(job_key, "update tmp_raw_data_new set 법정동시군구코드 = '36111' where 법정동시군구코드 = '36110'") < 0:
		items_job_fail(job_key, ym, counts[GET_CNT])
	if my_utils.execute_dml(job_key, "update tmp_raw_data_new set 법정동읍면동코드 = '32000' where 법정동시군구코드 = '41461' and (법정동읍면동코드 = '25931')") < 0:
		items_job_fail(job_key, ym, counts[GET_CNT])
	rows = my_utils.execute_dml(job_key, INSERT_RAW_DATA_ERROR)
	if rows > 0:
		update4 = my_utils.execute_dml(job_key, UPDATE_TMP_RAW_REGION_LEVEL4, (job_key,))
		update5 = my_utils.execute_dml(job_key, UPDATE_TMP_RAW_REGION_LEVEL5, (job_key,))
		if rows !=update4 + update5:
			logger.error("CHCK!!! tmp_raw_data_error Not All Updated : invalid = " + str(rows) + ", updated4 = " + str(update4) + ", updated5 = " + str(update5))
			items_job_fail(job_key, ym, counts[GET_CNT])

	return counts[GET_CNT]

GET_CNT = 0
INS_CNT = 1
DEL_CNT = 2
APT_CNT = 3
def save_sale_items(job_key, ym):

	my_utils.execute_dml(job_key, "truncate tmp_raw_data2_new")
#	my_utils.execute_dml(job_key, "truncate tmp_raw_data_error")

	counts = [0, 0, 0, 0]

	if reuse_tmp_data != True:
		counts[GET_CNT] = save_tmp_raw_data(job_key, ym)
		
	if my_utils.execute_dml(job_key, UPDATE_TMP_RAW_MADE_YEAR1) < 0:
		items_job_fail(job_key, ym, counts[GET_CNT])

	if my_utils.execute_dml(job_key, UPDATE_TMP_RAW_MADE_YEAR2) < 0:
		items_job_fail(job_key, ym, counts[GET_CNT])

	if my_utils.execute_dml(job_key, INSERT_TMP_RAW_DATA2, (ym,)) < 0:
		items_job_fail(job_key, ym, counts[GET_CNT])

	rows = my_utils.execute_dml(job_key, INSERT_APT_MASTER_NEW, (job_key,))
	if rows < 0:
		items_job_fail(job_key, ym, counts[GET_CNT])
	counts[APT_CNT] = rows

	rows = my_utils.execute_dml(job_key, UPDATE_TMP_RAW_DATA2)
	if rows < 0:
		items_job_fail(job_key, ym, counts[GET_CNT], counts[APT_CNT])

	rows = my_utils.execute_dml(job_key, SELECT_APT_ID_NULL)
	if rows != 0:
		logger.error("CHCK!!! apt_id null exists : " + str(rows))
		items_job_fail(job_key, ym, counts[GET_CNT], counts[APT_CNT])

	rows = my_utils.execute_dml(job_key, UPDATE_REGION3_APT_YN)
	if rows < 0:
		items_job_fail(job_key, ym, counts[GET_CNT], counts[APT_CNT])

	rows = my_utils.execute_dml(job_key, UPDATE_REGION2_APT_YN)
	if rows < 0:
		items_job_fail(job_key, ym, counts[GET_CNT], counts[APT_CNT])

	rows = my_utils.execute_dml(job_key, INSERT_RAW_DATA)
	if rows < 0:
		items_job_fail(job_key, ym, counts[GET_CNT], counts[APT_CNT])

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_ITEMS, (job_key,))
	if rows < 0:
		items_job_fail(job_key, ym, counts[GET_CNT], counts[APT_CNT])
	counts[INS_CNT] = rows

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_DELETED, (job_key, ym, ym))
	if rows < 0:
		items_job_fail(job_key, ym, counts[GET_CNT], counts[APT_CNT], counts[INS_CNT])
	elif rows > 0:
		rows = my_utils.execute_dml(job_key, DELETE_APT_SALE_ITEMS, (ym, job_key)) 
		if rows < 0:
			items_job_fail(job_key, ym, counts[GET_CNT], counts[APT_CNT], counts[INS_CNT])
		counts[DEL_CNT] = rows

	return counts

for ym in YMs: 
	ym_start_dt = datetime.datetime.now()
	job_key = JOB_PREFIX + "_" + ym_start_dt.strftime('%Y%m%d%H%M%S')
	logger.info(ym + " : starting..." + "stats_only = " + str(stats_only) + ", qbox_only = " + str(qbox_only))
	my_utils.job_start(JOB_NAME, job_key, ym)

	my_utils.execute_dml(job_key,"delete from tmp_ym where ym='"+ym+"'")
	my_utils.execute_dml(job_key,"insert into tmp_ym values('"+ym+"')")

	counts = [0, 0, 0, 0]
	if not (stats_only or qbox_only):
		counts = save_sale_items(job_key, ym)

	if not qbox_only:
		update_stats(job_key, ym)
	update_qbox_stats(job_key, ym)

	my_utils.job_finish(job_key, counts[GET_CNT], counts[APT_CNT], counts[INS_CNT], counts[DEL_CNT])

	ym_end_dt = datetime.datetime.now()
	logger.info(ym + " : Completed(" + str(ym_end_dt - ym_start_dt) + ") : " + str(counts[GET_CNT]))


logger.info("Completed!!")

os.remove(PID_FILE)

