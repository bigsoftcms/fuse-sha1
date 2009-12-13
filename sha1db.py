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
from fusesha1util import sha1sum, moveFile, sqliteConn, symlinkFile

from optparse import OptionParser

LOG_FILENAME = "LOG"
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)
		
class Sha1DB:
	# Creates a new Sha1DB.  If the database given does not exist, it will be created.
	def __init__(self, database):
		self.database = database

		dbExists = os.path.exists(database)
		
		if not dbExists:
			logging.info("Sha1DB initialized with connection string %s" % database)
			self._execSql("""create table if not exists files(
path varchar not null primary key,
chksum varchar not null,
symlink boolean default 0);""")
			
	def dedup(self, dupdir, doSymlink):
		""" Moves duplicate entries (based on checksum) into the dupdir.  Uses the entry's path to 
		reconstruct a subdirectory hierarchy in dupdir.  This will remove any common prefixes
		between dupdir and the file path itself so as to make a useful subdirectory structure.
		If doSymlink is true, then the original paths of the files that were moved will be symlinked 
		back to the canonical file; in addition, it will keep the file entry in the database rather than
		removing it."""
		logging.debug("De-duping database")
	
		if os.path.exists(dupdir) and not len(os.listdir(dupdir)) <= 0:
			raise Exception("%s is not empty; refusing to move files" % dupdir)
			
		try:
			pathmap = {} # store duplicate paths keyed by file checksum
			
			extra = ""
			if doSymlink:
				extra = "and symlink = 0"
			
			with sqliteConn(self.database) as cursor:
				cursor.execute("""select chksum, path from files where chksum in(
select chksum from files group by chksum 
having count(chksum) > 1) %s order by chksum;""" % extra)
				while(1):
					entry = cursor.fetchone()
					if entry == None: break
					
					(chksum, path) = entry
					if not chksum in pathmap: pathmap[chksum] = [] # ensure existence of list for checksum
					paths = pathmap[chksum]
					paths.append(path)
					
			with sqliteConn(self.database) as cursor:
				for chksum, paths in pathmap.iteritems():
					canonicalPath = paths[0]
					del paths[0] # we want to keep one file, so keep the first
					for path in paths:
						moveFile(path, dupdir, (not doSymlink)) # don't rm empty dirs if we are symlinking
						if not doSymlink:
							cursor.execute("delete from files where path = ?;", (path, ))
						else:
							cursor.execute("update files set symlink = 1 where path = ?;", (path, ))
							symlinkFile(canonicalPath, path)

		except Exception as einst:
			logging.error("Unable to de-dup database: %s" % einst)
			raise
		
	def vacuum(self):
		""" Check the paths in the database, removing entries for which no actual file exists """
		logging.info("Vacuuming database")
		
		try:
			paths = [] # store nonexistent paths
			with sqliteConn(self.database) as cursor:
				cursor.execute("select path from files;")
				while(1):
					entry = cursor.fetchone()
					if entry == None: break
					(path, ) = entry
					paths.append(path)
					
			if len(paths) > 0:
				with sqliteConn(self.database) as cursor:
					for path in paths:
						if not os.path.exists(path):
							logging.info("Removing entry for %s; file does not exist" % path)
							cursor.execute("delete from files where path = ?;", (path, ))
		except Exception as einst:
			logging.error("Unable to vacuum database: %s" % einst)
			raise

	def updateChecksum(self, path):
		""" Update/insert checksums for a given path.  If the path points at a symlink, the entry will 
		be marked as being a symlink."""
		
		isLink = 0
		if os.path.islink(path): isLink = 1
		
		with open(path, 'rb') as f:
			chksum = sha1sum(f)
			self._execSql("insert or replace into files(path, chksum, symlink) values(?, ?, ?);", (path, chksum, isLink))
	
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
		
		with sqliteConn(self.database) as cursor:
			try:
				if sqlargs != None: cursor.execute(sql, sqlargs)
				else: cursor.execute(sql)
			except Exception as einst:
				logging.error("Unable to exec %s with args: %s" % (sql, sqlargs, einst))
				raise
				
def main():
	usage = """%prog perform operations on the FUSE SHA1 filesystem database.  [options] database."""
	parser = OptionParser(usage = usage)
	parser.add_option("--dedup",
	 								  dest = "dupdir",
	 								  help = "Move duplicates into DUPDIR",
	 								  metavar="DUPDIR")
	
	parser.add_option("--symlink",
										action = "store_true",
										dest = "doSymlink",
										default = False,
										help = "Symlinks original paths for duplicates after moving them during --dedup.")
	
	parser.add_option("--vacuum",
								 		action = "store_true",
										dest = "vacuum",
										default = False,
										help = "Remove entries for nonexistent files")

	(options, args) = parser.parse_args()
	
	if len(args) != 1:
		parser.error("You must give the path to the SQLite database to use.")
	
	database = args[0]
	
	if not os.path.exists(database):
		parser.error("%s does not exist" % database)
		
	sha1db = Sha1DB(database)
	
	if None != options.dupdir:
		sha1db.dedup(options.dupdir, options.doSymlink)
		
	if None != options.vacuum:
		sha1db.vacuum()
	

if __name__ == '__main__':
	main()
