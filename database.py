# Import psql and file-loading packages.
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path

# Database module. Yeah, psycopg2 can probably do most of this...

def createDatabase(cursor, database):
	sqlPrepDatabase = 'DROP DATABASE IF EXISTS '+ database +';'
	sqlCreateDatabase = 'CREATE DATABASE '+ database +';'
	cursor.execute(sqlPrepDatabase)
	cursor.execute(sqlCreateDatabase)

def executeQueryFile(cursor, filePath):
	sql = Path(filePath).read_text()
	cursor.execute(sql)

def loadTable(cursor, table, filePath):
	with open(filePath, 'r') as f:
		cursor.copy_from(f, table, sep=',')

def outputQuery(cursor, query, fileName):
	cursor.execute(query)
	records = cursor.fetchall()
	with open(fileName, 'w') as f:
		for record in records:
			line = ''
			for field in record:
				line += ',' + field
			print(line[1:], file=f)

def outputQueryFile(cursor, filePath, fileName):
	query = Path(filePath).read_text()
	cursor.execute(query)
	records = cursor.fetchall()
	with open(fileName, 'w') as f:
		for record in records:
			line = ''
			for field in record:
				line += ',' + field
			print(line[1:], file=f)
