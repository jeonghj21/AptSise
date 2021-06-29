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

JOB_PREFIX = "STATS"
app = my_utils.init_app()
logger = my_utils.init_logger(JOB_PREFIX)

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
			 , round((sum(unit_price * cnt) / sum(cnt)), 2), round((sum(price * cnt) / sum(cnt)), 2), sum(cnt)
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

QBOX_FROMs = [" from region_info r, apt_sale_items a, apt_master m"
			 , " from region_info r, region_info r1, apt_sale_items a, apt_master m" 
			 , " from region_info r, region_info r1, region_info r2, apt_sale_items a, apt_master m"
			 , " from (select '0000000000' region_key, 0 level from dual) r, apt_sale_items a, apt_master m"]

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


def update_stats(job_key, ym):

	rows = my_utils.execute_dml(job_key, DELETE_APT_SALE_STATS_NEW, (ym,))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_ALL, (ym,))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_DANJI_Y, (ym,))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_LEVEL_2, (ym,))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_LEVEL_1, (ym,))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_SALE_STATS_LEVEL_0, (ym,))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, DELETE_APT_MA, (ym, ym))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_MA, (ym, ym))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, DELETE_APT_REGION_MA, (ym, ym))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	rows = my_utils.execute_dml(job_key, INSERT_APT_REGION_MA, (ym, ym))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)


def update_qbox_stats(job_key, ym):

	rows = my_utils.execute_dml(job_key, "delete from apt_qbox_stats where ym = %s", (ym,))
	if rows < 0:
		job_fail(0, 0, 0, 0, job_key, ym)

	my_utils.execute_dml(job_key, "SET GROUP_CONCAT_MAX_LEN = 4294967295")
	for i in range(0, 4):
		sql = INSERT_QBOX_STATS_1 + QBOX_FROMs[i] + QBOX_WHEREs[i] + QBOX_END
		rows = my_utils.execute_dml(job_key, sql, (ym,))
		if rows < 0:
			job_fail(0, 0, 0, 0, job_key, ym)

	for i in range(0, 4):
		sql = INSERT_QBOX_STATS_2 + QBOX_FROMs[i] + QBOX_WHEREs[i] + QBOX_END
		rows = my_utils.execute_dml(job_key, sql, (ym,))
		if rows < 0:
			job_fail(0, 0, 0, 0, job_key, ym)

for ym in YMs: 
	ym_start_dt = datetime.datetime.now()
	job_key = JOB_PREFIX + "_" + ym_start_dt.strftime('%Y%m%d%H%M%S')
	logger.info(ym + " : starting...")
	my_utils.job_start(JOB_NAME, job_key, ym)

	update_stats(job_key, ym)
	update_qbox_stats(job_key, ym)

	my_utils.job_finish(job_key, 0, 0, 0, 0)
	ym_end_dt = datetime.datetime.now()

	logger.info(ym + " : Completed(" + str(ym_end_dt - ym_start_dt) + ")")

logger.info("Completed!!")

