import pandas as pd 
import csv
import mysql.connector
import os


def connect_db ():

	config = {
	'user': os.environ['DB_TRUSTHOST_STB_USER'],
	'password': os.environ['DB_TRUSTHOST_STB_PASS'],
	'host': os.environ['DB_TRUSTHOST_STB_HOST'],
	'database': os.environ['DB_TRUSTHOST_STB_DB_NAME'],
	'raise_on_warnings': True,
	'use_pure': True
	}

	cnx = mysql.connector.connect(**config)

	return cnx

def send_data_db_sql (cnx, mass):
	cursor = cnx.cursor()
	
	query = ("INSERT INTO seo_pixel_tools_positions_stb "
	    "(date, ya_msk_3, ya_msk_10, ya_msk_30, ya_msk_100_plus, ya_nn_3, ya_nn_10, ya_nn_30, ya_nn_100_plus, g_3, g_10, g_30, g_100_plus) "
	    "VALUES (%(date)s, %(ya_msk_3)s, %(ya_msk_10)s, %(ya_msk_30)s, %(ya_msk_100_plus)s, %(ya_nn_3)s, %(ya_nn_10)s, %(ya_nn_30)s, %(ya_nn_100_plus)s, %(g_3)s, %(g_10)s, %(g_30)s, %(g_100_plus)s);")
	
	insert_data = {
	'date':mass[0],
	'ya_msk_3':mass[1],
	'ya_msk_10':mass[2],
	'ya_msk_30':mass[3],
	'ya_msk_100_plus':mass[4],
	'ya_nn_3':mass[5],
	'ya_nn_10':mass[6],
	'ya_nn_30':mass[7],
	'ya_nn_100_plus':mass[8],
	'g_3':mass[9],
	'g_10':mass[10],
	'g_30':mass[11],
	'g_100_plus':mass[12]
	}

	print(insert_data)
	cursor.execute(query, insert_data)
	cnx.commit()
	cursor.close()
	cnx.close()

def extract_data ():

	with open('file.csv', 'r') as f:
		csv_reader = csv.reader(f)
		for mass in csv_reader:
			cnx = connect_db()
			send_data_db_sql(cnx, mass)


extract_data()


