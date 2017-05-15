from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import numpy as np
import datetime
from pymongo import MongoClient
import re
import pprint

TERMS = [['1M', '1-month'], ['3M', '3-month'], ['6M', '6-month'], 
		 ['1Y', '1-year'], ['2Y', '2-year'], ['3Y', '3-year'], ['5Y', '5-year'], 
		 ['7Y', '7-year'], ['10Y', '10-year'], ['20Y', '20-year'], ['30Y', '30-year']]

euro_webpage_prefix = "http://sdw.ecb.europa.eu/quickview.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_"
us_rates_csv_file = "/Users/GW/Documents/investment_research/interest_rates/FRB_H15.csv"


def get_us_rates():
	us_rates_table = []
	us_rates_dict = []
	us_rates = []
	us_dates = []
	col_names = []
	us_rates_table = pd.read_csv(us_rates_csv_file, na_values={'ND'}, skiprows=(1,2,3,4,5))

	# Find column label by getting the number and term (i.e. month or year)
	col = 0
	while col < len(us_rates_table.axes[1]):
		if not re.search(r'([1-6]-month|[1-7]-year|[1-3]0-year)', us_rates_table.axes[1][col]):
			col_names.append("Date")
		else:
			pos = re.search(r'([1-6]-month|[1-7]-year|[1-3]0-year)', us_rates_table.axes[1][col])
			col_names.append(pos.group(0))
		col += 1
	
	us_rates_table.columns=col_names
	
	# Fix the years for dates prior to 1969
	count = 0
	for d in us_rates_table.iloc[:,0]:
		us_dates.append(datetime.datetime.strptime(us_rates_table.iloc[count,0], '%m/%d/%y'))
		if us_dates[count].year > 2061:
			us_dates[count] = datetime.datetime(us_dates[count].year-100, us_dates[count].month, us_dates[count].day,0,0)
		us_rates_table.iloc[count,0] = us_dates[count].strftime("%Y-%m-%d")
		count += 1

	# Create a list of dictionaries foe each date and term
	for date in us_rates_table.itertuples():
		for val in range(2,13):
			us_rates_dict.append({'region': 'usa', 'term': TERMS[val-2][1], 'date': date[1], 'rate': round(date[val],2)})

	export_to_database(us_rates_dict)


def get_euro_rates():
	''' Get Euro bond rates for all TERMS except 1-month, which is not reported. '''
	
	website = None
	new_date = None
	new_value = None

	for k in TERMS:
		if k[0] != "1M":
			euro_list = []
			euro_rates = []

			# Affix term to end of webpage string
			website = requests.get(euro_webpage_prefix + k[0])
			
			# Extract data from webpage
			soup = BeautifulSoup(website.text, 'lxml')
			data_table = soup.find(id="contentlimiter4")
			table_class = data_table.find("table", class_="tablestats")
			data = table_class.find_all('td')

			for td in data:
				
				try:
					if isinstance(datetime.datetime.strptime(td.text, '%Y-%m-%d'), datetime.datetime):
						new_date = (td.text.encode('utf-8'))
				except Exception as e:
					pass
				
				try:
					if float(td.text):
						new_value = (float(td.text))
						euro_rates.append([new_date, new_value])
				except Exception as e:
					pass
			
			# Create a list of dictionaries foe each date and term
			for v in euro_rates:
				euro_list.append({'region': 'euro', 'term': k[0], 'date': v[0], 'rate': v[1]})
			
			export_to_database(euro_list)


def export_to_database(data):
	''' Establish connection with MongoDB and insert data. '''

	client = MongoClient()
	db = client.market_data
	db.interest_rates.insert(data)


if __name__ == "__main__":
	get_euro_rates()
	get_us_rates()
