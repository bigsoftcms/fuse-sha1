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
	
	# Call to update/insert checksums for a given path
	def updateChecksum(self, path):
		with open(path, 'rb') as f:
			chksum = sha1sum(f)
			# this is super unsafe SQL, but since I consider this low security, it's probably OK
			self._execSql("insert or replace into files(path, chksum) values('%s', '%s')" % (path, chksum))
	
	# Call to remove the checksum/path entry for the given path from the database
	def removeChecksum(self, path):
		self._execSql("delete from files where path = '%s'" % path)
			
	# internal method used to run arbitrary SQL on the SQLite database
	def _execSql(self, sql):
		if not sql.endswith(";"): sql = sql + ";"
		logging.info("Running SQL '%s'" % sql)
		connection = None
		try:
			connection = sqlite.connect(self.database)
			cursor = None
			try:
				cursor = connection.cursor()
				cursor.execute(sql)
				cursor.close()
	
				connection.commit()
			except:
				logging.error("Unable to exec %s" % sql)
				cursor.close()
				connection.rollback()
		finally:
			if (connection is not None): connection.close()
