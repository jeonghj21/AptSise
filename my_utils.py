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

MAX_RETRY = 3
SLEEP_SEC = 3
TIMEOUT_SEC = 5

app = Flask(__name__)
def init_app():

	app.config.from_envvar('FLASK_CONFIG')

	app.engine = create_engine(app.config['DB_URL'], encoding = 'utf-8')

	return app

logger = logging.getLogger()
def init_logger(prefix):
	logger.setLevel(logging.INFO)
	fh = logging.FileHandler(filename = app.config['BASE_DIR'] + "/" + prefix + "_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".log")
	formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
	fh.setFormatter(formatter)
	logger.addHandler(fh)

	return logger

rows = 0

def get_items(URL, item_list, params, pageNo, NUM_OF_ROWS, job_key, columns, add_columns):
#	url = URL + "?LAWD_CD="+lawdCd+"&DEAL_YMD="+Ymd+"&serviceKey="+API_KEY+"&numOfRows="+NUM_OF_ROWS+"&pageNo="+str(pageNo)
	url = URL + "?numOfRows="+NUM_OF_ROWS+"&pageNo="+str(pageNo)

	for key in params.keys():
		url = url + "&" + key + "=" + params[key]

	count = 0
	try_count = 0
	content = ""
	success = False
	while not success:
		try:
			response = requests.get(url, timeout = TIMEOUT_SEC)
			content = response.content if response.status_code == 200 else ""
			success = True
			logger.info("get_items completed for : " + url)
		except Exception as e:
			try_count = try_count + 1
			if try_count == MAX_RETRY:
				return -1
			else:
				time.sleep(SLEEP_SEC)

	root = None
	try:
		root = ET.fromstring(content)
	except ET.ParseError as err:
		logger.exception("Response parse error")
		return -1

	header = root.find("header")
	if header == None or header.find("resultCode").text != "00":
		logger.error("Error response :" + "header missing" if header == None else header.find("resultMsg").text)
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

		for key in add_columns.keys():
			data[key] = add_columns[key]

		item_list.append(data)  

	if pageNo == 1:
		totalCount = int(root.find('body').find('totalCount').text)
		totalPage = int(totalCount / int(NUM_OF_ROWS)) + 1
		while pageNo < totalPage:
			item_cnt = get_items(URL, item_list, params, pageNo + 1, NUM_OF_ROWS, job_key, columns, add_columns)
			if item_cnt < 0:
				return -1
			count = count + item_cnt
			pageNo = pageNo + 1

	return count

INSERT_JOB_ERROR = "insert into job_error values(%s, %s, %s, %s)"

INSERT_JOB_LOG = "insert into job_log values(%s, %s, %s, now(), null, null, 0, 0, 0, 0)"

UPDATE_JOB_LOG_SUCCESS = """
	update job_log set 
		end_dt = now()
	  , result='Y'
	  , get_cnt = %s
	  , insert_cnt = %s
	  , delete_cnt = %s
	  , new_apt_cnt = %s
	 where job_key = %s
"""

UPDATE_JOB_LOG_FAIL = """
	update job_log 
	   set end_dt = now()
	   	 , result='N'
	  	 , get_cnt = %s
	  	 , insert_cnt = %s
	  	 , delete_cnt = %s
	  	 , new_apt_cnt = %s
	 where job_key = %s
"""

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

def job_fail(get_cnt, ins_cnt, del_cnt, apt_cnt, job_key, ym, del_data = True, del_stat = True):
	execute_dml(job_key, UPDATE_JOB_LOG_FAIL, (get_cnt, ins_cnt, del_cnt, apt_cnt, job_key), False)
	if ym != None and del_data:
		execute_dml(job_key, "delete from raw_data_new where ym = %s", (ym,))
		execute_dml(job_key, "delete from apt_sale_items where job_key = %s", (job_key,))
		execute_dml(job_key, "delete from apt_master where job_key = %s", (job_key,))
		execute_dml(job_key, "delete from apt_sale_deleted where ym = %s", (ym,))
	if ym != None and del_stat:
		execute_dml(job_key, "delete from apt_sale_stats where ym = %s", (ym,))
		execute_dml(job_key, "delete from apt_ma_new where ym = %s", (ym,))
		execute_dml(job_key, "delete from apt_region_ma where ym = %s", (ym,))
	execute_dml(job_key, "delete from apt_qbox_stats where ym = %s", (ym,))

	sys.exit(1)

def job_start(job_name, job_key, ym):
	execute_dml(job_key, INSERT_JOB_LOG, (job_name, job_key, ym))

def job_finish(job_key, get_cnt, apt_cnt, ins_cnt, del_cnt):
	execute_dml(job_key, UPDATE_JOB_LOG_SUCCESS, (get_cnt, ins_cnt, del_cnt, apt_cnt, job_key))


