import hashlib
# Calculate an SHA1 hex digest for a file
# Copyright (C) 2009 Chris Bouzek  <bouzekc@gmail.com>
#
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

import logging
import os

LOG_FILENAME = "LOG"
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

def sha1sum(fobj):
	'''Returns a SHA1 hash for an object with read() method.'''
	kb = 1024
	chunksize = 100 * kb

	m = hashlib.sha1()
	while True:
		d = fobj.read(chunksize)
		if not d:
			break
		m.update(d)
	return m.hexdigest()
	
def moveFile(path, dstdir):
	"""Moves the file with the given path to the dstdir, removing any common prefixes between path 
	and dstdir.  After moving, this will remove the parent directory for path if it is empty."""
	newpath = os.path.abspath(path)
	dstdir = os.path.abspath(dstdir)
	# remove any common prefixes so that we can create a directory structure
	prefix = os.path.commonprefix([dstdir, newpath])
	if len(prefix) > 1: newpath = newpath.replace(prefix, '', 1)
	newpath = os.path.join(dstdir, newpath)
	newparent = os.path.dirname(newpath)
	if not os.path.exists(newparent): os.makedirs(newparent)
	if not os.path.exists(newpath): os.rename(path, newpath)
	
	oldparent = os.path.dirname(path)
	if len(os.listdir(oldparent)) <= 0:
		os.rmdir(oldparent)
	
# Wraps a code block so that if an exception occurs, it is logged
class ewrap:
	def __init__(self, funcName):
		self.funcName = funcName
	def __enter__(self):
		return self.funcName
	def __exit__(self, type, value, traceback):
		if not value is None:
			logging.error("!! Exception in %s: %s" % (self.funcName, value))
