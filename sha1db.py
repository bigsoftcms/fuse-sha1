#!/usr/bin/python
# utility functions for dealing with MD5 hashing
# Copyright (C) 2009 Chris Bouzek  <bouzekc@gmail.com>
#
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

import os
import logging
from fusesha1util import sha1sum, moveFile

from pysqlite2 import dbapi2 as sqlite
from optparse import OptionParser

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
			
	def dedup(self, dupdir):
		""" Moves duplicate entries (based on checksum) into the dupdir.  Uses the entry's path to 
		reconstruct a subdirectory hierarchy in dupdir.  This will remove any common prefixes
		between dupdir and the file path itself so as to make a useful subdirectory structure."""
		logging.info("De-duping database")
		pathmap = {}
		with sqlite.connect(self.database) as connection:
			cursor = None
			try:
				cursor = connection.cursor()
				cursor.execute("""select chksum, path from files where chksum in(
select chksum from files group by chksum 
having count(chksum) > 1) order by chksum;""")
				while(1):
					entry = cursor.fetchone()
					
					if entry == None: break
					
					#
					(chksum, path) = entry
					if not chksum in pathmap:
						pathmap[chksum] = []
					paths = pathmap[chksum]
					paths.append(path)
					
				cursor.close()
				
				cursor = connection.cursor()
				for chksum, paths in pathmap.iteritems():
					del paths[0]
					for path in paths:
						moveFile(path, dupdir)
						cursor.execute("delete from files where path = ?;", (path, ))
				cursor.close()
				#
			except Exception as einst:
				logging.error("Unable to de-dup database: %s" % einst)
				if connection != None: connection.rollback()
				raise
			else:
				connection.commit()
			finally:
				if cursor != None: cursor.close()
		
	def vacuum(self):
		""" Check the paths in the database, removing entries for which no actual file exists """
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
						
						#
						(path, ) = entry
					
						if not os.path.exists(path):
							logging.info("Removing entry for %s; file does not exist" % path)
							cursor.execute("delete from files where path = ?;", (path, ))
							
						#
							
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

	def updateChecksum(self, path):
		""" Update/insert checksums for a given path """
		with open(path, 'rb') as f:
			chksum = sha1sum(f)
			self._execSql("insert or replace into files(path, chksum) values(?, ?);", (path, chksum))
	
	def removeChecksum(self, path):
		""" Remove the checksum/path entry for the given path from the database """
		self._execSql("delete from files where path = ?;", (path, ))
						
	# Makes sure the SQL statement has a "; at the end"
	def _formatSql(self, sql):
		if not sql.endswith(";"): sql = sql + ";"
		return sql
		
	# internal method used to run arbitrary SQL on the SQLite database
	def _execSql(self, sql, sqlargs = None):
		sql = self._formatSql(sql)
		logging.debug("Running SQL %s with args %s" % (sql, sqlargs))
		with sqlite.connect(self.database) as connection:
			cursor = None
			try:
				cursor = connection.cursor()
				
				#
				if sqlargs != None:
					cursor.execute(sql, sqlargs)
				else:
					cursor.execute(sql)
				#
				
				cursor.close()
			except Exception as einst:
				logging.error("Unable to exec %s with args: %s" % (sql, sqlargs, einst))
				if connection != None: connection.rollback()
			else:
				connection.commit()
			finally:
				if cursor != None: cursor.close()
				
def main():
	usage = """%prog perform operations on the FUSE SHA1 filesystem database.  [options] database."""
	parser = OptionParser(usage = usage)
	parser.add_option("--dedup",
	 								  dest = "dupdir",
	 								  help = "Move duplicates into DUPDIR",
	 								  metavar="DUPDIR")

	(options, args) = parser.parse_args()
	
	if len(args) != 1:
		parser.error("You must give the path to the SQLite database to use.")
	
	database = args[0]
	
	if not os.path.exists(database):
		parser.error("%s does not exist" % database)
	
	if None != options.dupdir:
		sha1db = Sha1DB(database)
		sha1db.dedup(options.dupdir)
	

if __name__ == '__main__':
	main()
