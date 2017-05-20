import matplotlib.pyplot as plt
import pymongo
from pymongo import MongoClient
import pandas as pd
from govt_interest_rates import TERMS
import pprint
import seaborn as sns


def list_of_user_prompts(help, *region):

	if help == 'regions':
		print "usa, euro"
	elif help == 'terms':
		print [t[0] for t in TERMS]


def user_prompt():
	''' Prompt the user to provide the information they want to plot'''
	region = 'regions'
	term = 'terms'

	while region == 'regions':
		region = raw_input("Enter the region to graph (type 'regions' to get available " \
						   "options): ").lower()
		if region == 'regions':
			list_of_user_prompts(region)

	while term == 'terms':
		term = raw_input("Enter the Term (press enter to get available options): ").upper()
		if term == 'terms':
			list_of_user_prompts(term, region)

	start_date = raw_input("Enter the start date as yyyy-mm-dd: ")

	get_data(region, term, start_date)


def get_data(region, term, date):
	''' Make a call to MongoDB'''

	db = establish_DB_conn()

	query = db.find({'region': region, 'date': {'$gt': date}, 'term': term}, {"_id":0, "date":1, "rate":1})		

	df = pd.DataFrame(list(query))
	df.set_index('date', inplace=True)
	df.sort_index(ascending=True, inplace=True)

	#pprint.pprint (df)
	
	chart_data(df, region, term)


def establish_DB_conn():
	client = MongoClient()
	return client.market_data.interest_rates


def chart_data(df, region, term):
	''' Graph the selected dataset'''

	ax = df.plot()
	
	ax.set_xlabel("Date", fontsize=12, fontweight='bold')
	ax.set_ylabel("Rate", fontsize=12, fontweight='bold')

	plt.title(region.upper() + ' ' + term+" Rate", fontsize=15, fontweight='bold')
	plt.xticks(rotation=15, fontsize=8, fontweight='bold')
	plt.yticks(fontsize=8, fontweight='bold')
	plt.tight_layout(pad=1.1)
	plt.show()


if __name__ == "__main__":
	user_prompt()
