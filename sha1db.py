#!/usr/bin/python
# utility functions for dealing with MD5 hashing
# Copyright (C) 2009 Chris Bouzek  <coldfusion78@gmail.com>
#
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

import os
import logging
from fusesha1util import sha1sum

from pysqlite2 import dbapi2 as sqlite

LOG_FILENAME = "LOG"
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)
		
class Sha1DB:
	# Initializes the database given by the constructor parameter database. If it already exists, 
	# this is a no-op.
	def __init__(self, database):
		self.database = database

		dbExists = os.path.exists(database)
		
		if not dbExists:
			logging.info("Sha1DB initialized with connection string %s" % database)
			self._execSql("""create table if not exists files(
path varchar not null primary key,
chksum varchar not null);""")
	
	# Check the paths in the database, removing entries for which no actual file exists
	def vacuum(self):
		logging.info("Vacuuming database")
		with sqlite.connect(self.database) as connection:
			querycursor = None
			cursor = None
			try:
				querycursor = connection.cursor()
				try:
					cursor = connection.cursor()
					querycursor.execute("select path from files;")
					while(1):
						entry = querycursor.fetchone()
						if entry == None: break
						(path, ) = entry
					
						if not os.path.exists(path):
							logging.info("Removing entry for %s; file does not exist" % path)
							cursor.execute("delete from files where path = '%s';" % path)
							
					querycursor.close()
					cursor.close()
				except Exception as einst:
					logging.error("Unable to vacuum database: %s" % einst)
					if connection != None: connection.rollback()
					raise
				else:
					connection.commit()
				finally:
					if cursor != None: cursor.close()
			finally:
				if querycursor != None: querycursor.close()
		
	# Call to update/insert checksums for a given path
	def updateChecksum(self, path):
		with open(path, 'rb') as f:
			chksum = sha1sum(f)
			# this is super unsafe SQL, but since I consider this low security, it's probably OK
			self._execSql("insert or replace into files(path, chksum) values('%s', '%s');" % (path, chksum))
	
	# Call to remove the checksum/path entry for the given path from the database
	def removeChecksum(self, path):
		self._execSql("delete from files where path = '%s';" % path)
		
	# Makes sure the SQL statement has a "; at the end"
	def _formatSql(self, sql):
		if not sql.endswith(";"): sql = sql + ";"
		return sql
		
	# internal method used to run arbitrary SQL on the SQLite database
	def _execSql(self, sql):
		sql = self._formatSql(sql)
		logging.info("Running SQL '%s'" % sql)
		with sqlite.connect(self.database) as connection:
			cursor = None
			try:
				cursor = connection.cursor()
				cursor.execute(sql)
				cursor.close()
			except Exception as einst:
				logging.error("Unable to exec %s: %s" % (sql, einst))
				if connection != None: connection.rollback()
			else:
				connection.commit()
			finally:
				if cursor != None: cursor.close()
