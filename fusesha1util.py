import hashlib
# Calculate an SHA1 hex digest for a file
# Copyright (C) 2009 Chris Bouzek  <coldfusion78@gmail.com>
#
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

import logging

LOG_FILENAME = "LOG"
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

def sha1sum(fobj):
	'''Returns a SHA1 hash for an object with read() method.'''
	m = hashlib.sha1()
	while True:
		d = fobj.read(1024)
		if not d:
			break
		m.update(d)
	return m.hexdigest()
	
# Wraps a code block so that if an exception occurs, it is logged
class ewrap:
	def __init__(self, funcName):
		self.funcName = funcName
	def __enter__(self):
		return self.funcName
	def __exit__(self, type, value, traceback):
		if not value is None:
			logging.info("!! Exception in %s: %s" % (self.funcName, value))
