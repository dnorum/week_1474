# Import psql and file-loading packages.
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path

# Import package for pre-run cleanup.
import os

# Import local database functions.
import database

# Define database name.
databaseName = 'week_1474'

# Connect to the starting database.
connection = psycopg2.connect(	dbname='python'
			,	user='python'
			,	host='localhost'
			,	password='python'	)
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = connection.cursor()

# Create the database for the scripts, then close the connection.
database.createDatabase(cursor, databaseName)
connection.close()

# Connect to the scripts' database.
connection = psycopg2.connect(	dbname=databaseName
			,	user='python'
			,	host='localhost'
			,	password='python'	)
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = connection.cursor()

# Create the corpus table to store the raw hyphenates.
database.executeQueryFile(cursor, 'createTableCorpus.sql')

# Load the corpus file into the table.
database.loadTable(cursor, 'corpus', 'corpus.csv')

# Lowercase all entries.
database.executeQueryFile(cursor, 'lowercaseCorpusWords.sql')

# Break out the corpus into components.
database.executeQueryFile(cursor, 'createComponentsTable.sql')

# Get the list of days.
cursor.execute('SELECT DISTINCT day FROM components;')
days = cursor.fetchall()

# For each day, print the list of components.
for row in days:
	query = "SELECT DISTINCT component FROM components WHERE day = '" + row[0] + "';"
	fileName = row[0].lower().replace(' ', '_') + '.csv'
	if os.path.exists(fileName):
		os.remove(fileName)
	database.outputQuery(cursor, query, fileName)

# Close the connection.
connection.close()
