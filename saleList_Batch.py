from flask import Flask
from sqlalchemy import create_engine, text
import pandas as pd
import xml.etree.ElementTree as ET
import os
import requests
import datetime
from datetime import date

app = Flask(__name__)
app.config.from_envvar('FLASK_CONFIG')

app.engine = create_engine(app.config['DB_URL'], encoding = 'utf-8')

API_KEY = "kowxleiR8vdBv9Du%2BV5P%2BiRZkzWVDZZi9P3BzCA8etSREsXh991q8cu4AhU1dsAFxe3btGhEA1%2FupLgRLn1iQw%3D%3D"
NUM_OF_ROWS = "1000"
URL = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"

def get_items(item_list, lawdCd, Ymd, pageNo):
	url = URL + '?LAWD_CD=' + lawdCd + "&DEAL_YMD=" + Ymd + "&serviceKey=" + API_KEY + "&numOfRows=" + NUM_OF_ROWS + "&pageNo=" + str(pageNo)

	response = requests.get(url)
	root = ET.fromstring(response.content)
	for child in root.find('body').find('items'):
		elements = child.findall('*')
		data = {}
		for element in elements:
			tag = element.tag.strip()
			text = element.text.strip()
			# print tag, text
			data[tag] = text
		item_list.append(data)  

	if pageNo == 1:
		totalCount = int(root.find('body').find('totalCount').text)
		totalPage = int(totalCount / int(NUM_OF_ROWS)) + 1
		while pageNo < totalPage:
			get_items(item_list, lawdCd, Ymd, pageNo + 1)
			pageNo = pageNo + 1

YMs = []
year = date.today().year
month = date.today().month
i = 0
while i < 3:
	YMs.append("%04d%02d" %(year,month))
	month = month - 1
	if month < 1:
		month = 12 + month
		year = year - 1
	i = i + 1

items_list = []

sql = "select * from ref_region"
with app.engine.connect() as connection:
	result = connection.execute(text(sql))

items_list = []
for r in result:
	for YM in YMs:
		print("data gathering... : " + r['region_cd'] + ", " + YM)
		get_items(items_list, r['region_cd'], YM, 1)

items = pd.DataFrame(items_list) 

now = datetime.datetime.now()

items.to_csv(os.path.join("%s_%s-%s.csv" %(YMs[0], YMs[len(YMs)-1], now.strftime('%Y%m%d%H%M%S'))), index=False,encoding="euc-kr")


