#!/usr/bin/env python

# SHA1 checksum filesystem.  Calculates checksums for all files, storing them in a database given
# by the --database argument.  Any files removed/added/modified via this script will have
# their checksums updated.
#
# Modified version of Xmp.py, original copyrights as follows:
#    Copyright (C) 2001  Jeff Epler  <jepler@unpythonic.dhs.org>
#    Copyright (C) 2006  Csaba Henk  <csaba.henk@creo.hu>
#
# Docstring comments originally taken from templatefs.py by Matt Giuca
#
# SHA1 additions are Copyright (C) 2009  Chris Bouzek  <coldfusion78@gmail.com>
#
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

import os, sys
from errno import *
from stat import *
import fcntl
# pull in some spaghetti to make this stuff work without fuse-py being installed
try:
	import _find_fuse_parts
except ImportError:
	pass
import fuse
from fuse import Fuse

from pysqlite2 import dbapi2 as sqlite
import logging
import hashlib

LOG_FILENAME = "LOG"
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

if not hasattr(fuse, '__version__'):
	raise RuntimeError, \
		"your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)

fuse.feature_assert('stateful_files', 'has_init')

def flag2mode(flags):
	md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
	m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

	if flags | os.O_APPEND:
		m = m.replace('w', 'a', 1)

	return m
	
def flag2accessflag(flags):
	md = {os.O_RDONLY: os.R_OK, os.O_WRONLY: os.W_OK, os.O_RDWR: (os.W_OK | os.R_OK)}
	m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

	return m

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


class Sha1FS(Fuse):
	def __init__(self, *args, **kw):
		Fuse.__init__(self, *args, **kw)

		# do stuff to set up your filesystem here, if you want
		#import thread
		#thread.start_new_thread(self.mythread, ())
		self.root = '/'
		
		self.parser.add_option("--database",
													 dest = "database",
													 help = "location of SQLite checksum database (required)",
													 metavar="DATABASE")

#    def mythread(self):
#
#        """
#        The beauty of the FUSE python implementation is that with the python interp
#        running in foreground, you can have threads
#        """
#        print "mythread: started"
#        while 1:
#            time.sleep(120)
#            print "mythread: ticking"

	def execSql(self, sql):
		connection = None
		try:
			if not sql.endswith(";"): sql = sql + ";"
			opts, args = self.cmdline
			#logging.info("Running SQL %s for database %s" % (sql, opts.database))
			connection = sqlite.connect(opts.database)
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

	def getattr(self, path):
		"""
		Retrieves information about a file (the "stat" of a file).
		Returns a fuse.Stat object containing details about the file or
		directory.
		Returns -errno.ENOENT if the file is not found, or another negative
		errno code if another error occurs.
		"""
		with ewrap("getattr"):
			logging.debug("getattr: %s" % path)
			return os.lstat("." + path)

	def readlink(self, path):
		"""
		Get the target of a symlink.
		Returns a bytestring with the contents of a symlink (its target).
		May also return an int error code.
		"""
		with ewrap("readlink"):
			logging.debug("readlink: %s" % path)
			return os.readlink("." + path)

	def readdir(self, path, offset):
		"""
		Generator function. Produces a directory listing.
		Yields individual fuse.Direntry objects, one per file in the
		directory. Should always yield at least "." and "..".
		Should yield nothing if the file is not a directory or does not exist.
		(Does not need to raise an error).
		
		offset: I don't know what this does, but I think it allows the OS to
		request starting the listing partway through (which I clearly don't
		yet support). Seems to always be 0 anyway.
		"""
		with ewrap("readdir"):
			logging.debug("readdir: %s (offset %s)" % (path, offset))
			for e in os.listdir("." + path):
				yield fuse.Direntry(e)

	def unlink(self, path):
		"""Deletes a file."""
		with ewrap("unlink"):
			logging.info("unlink: %s" % path)
			os.unlink("." + path)

	def rmdir(self, path):
		"""Deletes a directory."""
		with ewrap("rmdir"):
			logging.debug("rmdir: %s" % path)
			os.rmdir("." + path)

	def symlink(self, target, name):
		"""
		Creates a symbolic link from path to target.
		
		The 'name' is a regular path like any other method (absolute, but
		relative to the filesystem root).
		The 'target' is special - it works just like any symlink target. It
		may be absolute, in which case it is absolute on the user's system,
		NOT the mounted filesystem, or it may be relative. It should be
		treated as an opaque string - the filesystem implementation should not
		ever need to follow it (that is handled by the OS).
		
		Hence, if the operating system creates a link FROM this system TO
		another system, it will call this method with a target pointing
		outside the filesystem.
		If the operating system creates a link FROM some other system TO this
		system, it will not touch this system at all (symlinks do not depend
		on the target system unless followed).
		"""
		with ewrap("symlink"):
			logging.debug("symlink: target %s, name: %s" % (target, name))
			os.symlink(target, "." + name)

	def rename(self, old, new):
		"""
		Moves a file from old to new. (old and new are both full paths, and
		may not be in the same directory).
		
		Note that both paths are relative to the mounted file system.
		If the operating system needs to move files across systems, it will
		manually copy and delete the file, and this method will not be called.
		"""
		with ewrap("rename"):
			logging.debug("rename: target %s, name: %s" % (old, new))
			os.rename("." + old, "." + new)

	def link(self, target, name):
		"""
		Creates a hard link from name to target. Note that both paths are
		relative to the mounted file system. Hard-links across systems are not
		supported.
		"""
		with ewrap("link"):
			logging.debug("link: target %s, name: %s" % (target, name))
			os.link("." + target, "." + name)

	def chmod(self, path, mode):
		"""Changes the mode of a file or directory."""
		with ewrap("chmod"):
			logging.debug("chmod: %s (mode %s)" % (path, oct(mode)))
			os.chmod("." + path, mode)

	def chown(self, path, user, group):
		"""Changes the owner of a file or directory."""
		with ewrap("chown"):
			logging.debug("chown: %s (uid %s, gid %s)" % (path, user, group))
			os.chown("." + path, user, group)

	def truncate(self, path, len):
		with ewrap("truncate"):
			with file("." + path, "a") as f:
				f.truncate(len)

	def mknod(self, path, mode, rdev):
		"""
		Creates a non-directory file (or a device node).
		mode: Unix file mode flags for the file being created.
		rdev: Special properties for creation of character or block special
				devices (I've never gotten this to work).
				Always 0 for regular files or FIFO buffers.
		"""
		# Note: mode & 0770000 gives you the non-permission bits.
		# Common ones:
		# S_IFREG:  0100000 (A regular file)
		# S_IFIFO:  010000  (A fifo buffer, created with mkfifo)
		
		# Potential ones (I have never seen them):
		# Note that these could be made by copying special devices or sockets
		# or using mknod, but I've never gotten FUSE to pass such a request
		# along.
		# S_IFCHR:  020000  (A character special device, created with mknod)
		# S_IFBLK:  060000  (A block special device, created with mknod)
		# S_IFSOCK: 0140000 (A socket, created with mkfifo)
		
		# Also note: You can use self.GetContext() to get a dictionary
		#   {'uid': ?, 'gid': ?}, which tells you the uid/gid of the user
		#   executing the current syscall. This should be handy when creating
		#   new files and directories, because they should be owned by this
		#   user/group.
		with ewrap("mknod"):
			logging.debug("mknod: %s (mode %s, rdev %s)" % (path, oct(mode), rdev))
			os.mknod("." + path, mode, rdev)
		#connection = sqlite.connect(database)
		#cursor = connection.cursor()
		#cursor.execute("""insert into files(path,chksum) values('1','1');""")
		#cursor.close()

		#connection.commit
		#connection.close()

	def mkdir(self, path, mode):
		"""
		Creates a directory.
		mode: Unix file mode flags for the directory being created.
		"""
		# Note: mode & 0770000 gives you the non-permission bits.
		# Should be S_IDIR (040000); I guess you can assume this.
		# Also see note about self.GetContext() in mknod.
		with ewrap("mkdir"):
			logging.debug("mkdir: %s (mode %s)" % (path, oct(mode)))
			os.mkdir("." + path, mode)

	def utime(self, path, times):
		"""
		Sets the access and modification times on a file.
		times: (atime, mtime) pair. Both ints, in seconds since epoch.
		Deprecated in favour of utimens.
		"""
		with ewrap("utime"):
			atime, mtime = times
			logging.debug("utime: %s (atime %s, mtime %s)" % (path, atime, mtime))
			os.utime("." + path, times)

#    The following utimens method would do the same as the above utime method.
#    We can't make it better though as the Python stdlib doesn't know of
#    subsecond preciseness in acces/modify times.
#
#    def utimens(self, path, ts_acc, ts_mod):
#      os.utime("." + path, (ts_acc.tv_sec, ts_mod.tv_sec))

	def access(self, path, flags):
		"""
		Checks permissions for accessing a file or directory.
		mode: As described in man 2 access (Linux Programmer's Manual).
				Either os.F_OK (test for existence of file), or ORing of
				os.R_OK, os.W_OK, os.X_OK (test if file is readable, writable and
				executable, respectively. Must pass all tests).
		Should return 0 for "allowed", or -errno.EACCES if disallowed.
		May not always be called. For example, when opening a file, open may
		be called and access avoided.
		"""
		with ewrap("access"):
			logging.info("access: %s (flags %s)" % (path, oct(flags)))
			if not os.access("." + path, flag2accessflag(flags)):
				return -EACCES
			else:
				return 0

#    This is how we could add stub extended attribute handlers...
#    (We can't have ones which aptly delegate requests to the underlying fs
#    because Python lacks a standard xattr interface.)
#
#    def getxattr(self, path, name, size):
#        val = name.swapcase() + '@' + path
#        if size == 0:
#            # We are asked for size of the value.
#            return len(val)
#        return val
#
#    def listxattr(self, path, size):
#        # We use the "user" namespace to please XFS utils
#        aa = ["user." + a for a in ("foo", "bar")]
#        if size == 0:
#            # We are asked for size of the attr list, ie. joint size of attrs
#            # plus null separators.
#            return len("".join(aa)) + len(aa)
#        return aa

	def statfs(self):
		"""
		Should return an object with statvfs attributes (f_bsize, f_frsize...).
		Eg., the return value of os.statvfs() is such a thing (since py 2.2).
		If you are not reusing an existing statvfs object, start with
		fuse.StatVFS(), and define the attributes.

		To provide usable information (ie., you want sensible df(1)
		output, you are suggested to specify the following attributes:

				- f_bsize - preferred size of file blocks, in bytes
				- f_frsize - fundamental size of file blcoks, in bytes
						[if you have no idea, use the same as blocksize]
				- f_blocks - total number of blocks in the filesystem
				- f_bfree - number of free blocks
				- f_files - total number of file inodes
				- f_ffree - nunber of free file inodes
		"""
		with ewrap("statfs"):
			return os.statvfs(".")

	def fsinit(self):
		"""
		Will be called after the command line arguments are successfully
		parsed. It doesn't have to exist or do anything, but as options to the
		filesystem are not available in __init__, fsinit is more suitable for
		the mounting logic than __init__.
		
		To access the command line passed options and nonoption arguments, use
		cmdline.
		
		The mountpoint is not stored in cmdline.
		"""
		with ewrap("fsinit"):
			logging.debug("Nonoption arguments: " + str(self.cmdline[1]))
			
			
			#self.xyz = self.cmdline[0].xyz
			#if self.xyz != None:
			#		logging.debug("xyz set to '" + self.xyz + "'")
			#else:
			#		logging.debug("xyz not set")
			
			os.chdir(self.root)
			logging.debug("Filesystem %s mounted" % self.root)

	### FILE OPERATION METHODS ###
	# Methods in this section are operations for opening files and working on
	# open files.
	# "open" and "create" are methods for opening files. They *may* return an
	# arbitrary Python object (not None or int), which is used as a file
	# handle by the methods for working on files.
	# All the other methods (fgetattr, release, read, write, fsync, flush,
	# ftruncate and lock) are methods for working on files. They should all be
	# prepared to accept an optional file-handle argument, which is whatever
	# object "open" or "create" returned.
	##################################
	def open(self, path, flags):
		"""
		Open a file for reading/writing, and check permissions.
		flags: As described in man 2 open (Linux Programmer's Manual).
				ORing of several access flags, including one of os.O_RDONLY,
				os.O_WRONLY or os.O_RDWR. All other flags are in os as well.
		
		On success, *may* return an arbitrary Python object, which will be
		used as the "fh" argument to all the file operation methods on the
		file. Or, may just return None on success.
		On failure, should return a negative errno code.
		Should return -errno.EACCES if disallowed.
		"""
		with ewrap("open"):
			mode = flag2mode(flags)
			logging.debug("open: %s (flags %s) (mode %s)" % (path, oct(flags), mode))
	
			fh = os.fdopen(os.open("." + path, flags), flag2mode(flags))
			
			if fh is None:
				return -ENOENT
			
			context = self.GetContext()
			accessflags = flag2accessflag(flags)
			#if not fh.stat.check_permission(context['uid'], context['gid'], accessflags):
			if not os.access("." + path, accessflags):
				return -EACCES
	
			return fh
		
		
	# def create(self, path, mode, rdev):
		# """
		# Creates a file and opens it for writing.
		# Will be called in favour of mknod+open, but it's optional (OS will
		# fall back on that sequence).
		# mode: Unix file mode flags for the file being created.
		# rdev: Special properties for creation of character or block special
				# devices (I've never gotten this to work).
				# Always 0 for regular files or FIFO buffers.
		# See "open" for return value.
		# """
		# logging.debug("create: %s (mode %s, rdev %s)" % (path,oct(mode),rdev))
		# self.mknod(path, mode, rdev)
		# return self.open(path, flags)

	def read(self, path, size, offset, fh=None):
		"""
		Get all or part of the contents of a file.
		size: Size in bytes to read.
		offset: Offset in bytes from the start of the file to read from.
		Does not need to check access rights (operating system will always
		call access or open first).
		Returns a byte string with the contents of the file, with a length no
		greater than 'size'. May also return an int error code.
		
		If the length of the returned string is 0, it indicates the end of the
		file, and the OS will not request any more. If the length is nonzero,
		the OS may request more bytes later.
		To signal that it is NOT the end of file, but no bytes are presently
		available (and it is a non-blocking read), return -errno.EAGAIN.
		If it is a blocking read, just block until ready.
		"""
		with ewrap("read"):
			logging.debug("read: %s (size %s, offset %s, fh %s)"
					% (path, size, offset, fh))
			fh.seek(offset)
			return fh.read(size)
		
	def write(self, path, buf, offset, fh=None):
		"""
		Write over part of a file.
		buf: Byte string containing the text to write.
		offset: Offset in bytes from the start of the file to write to.
		Does not need to check access rights (operating system will always
		call access or open first).
		Should only overwrite the part of the file from offset to
		offset+len(buf).
		
		Must return an int: the number of bytes successfully written (should
		be equal to len(buf) unless an error occured). May also be a negative
		int, which is an errno code.
		"""
		with ewrap("write"):
			logging.debug("write: %s (offset %s, fh %s)" % (path, offset, fh))
			logging.debug("  buf: %r" % buf)
			fh.seek(offset)
			fh.write(buf)
			return len(buf)
		
	def fgetattr(self, path, fh=None):
		"""
		Retrieves information about a file (the "stat" of a file).
		Same as Fuse.getattr, but may be given a file handle to an open file,
		so it can use that instead of having to look up the path.
		"""
		with ewrap("fgetattr"):
			logging.debug("fgetattr: %s (fh %s)" % (path, fh))
			return os.fstat(fh.fileno())
		
	def ftruncate(self, path, size, fh=None):
		"""
		Shrink or expand a file to a given size.
		Same as Fuse.truncate, but may be given a file handle to an open file,
		so it can use that instead of having to look up the path.
		"""
		with ewrap("ftruncate"):
			logging.debug("ftruncate: %s (size %s, fh %s)" % (path, size, fh))
			fh.truncate(size)
		
	def _fflush(self, fh):
		if 'w' in fh.mode or 'a' in fh.mode:
			fh.flush()
		
	def flush(self, path, fh=None):
		"""
		Flush cached data to the file system.
		This is NOT an fsync (I think the difference is fsync goes both ways,
		while flush is just one-way).
		"""
		with ewrap("flush"):
			logging.debug("flush: %s (fh %s)" % (path, fh))
			self._fflush(fh)
			# cf. xmp_flush() in fusexmp_fh.c
			os.close(os.dup(fh.fileno()))
		
	def release(self, path, flags, fh=None):
		"""
		Closes an open file. Allows filesystem to clean up.
		flags: The same flags the file was opened with (see open).
		"""
		with ewrap("release"):
			logging.info("release: %s (flags %s, fh %s)" % (path, oct(flags), fh))
			fh.close()
			with open("." + path, 'r') as f:
				chksum = sha1sum(f)
				# this is super unsafe SQL, but since I consider this low security, it's probably OK
				self.execSql("insert or replace into files(path, chksum) values('%s', '%s')" % (path, chksum))
		
	def fsync(self, path, datasync, fh=None):
		"""
		Synchronises an open file.
		datasync: If True, only flush user data, not metadata.
		"""
		with ewrap("fsync"):
			logging.debug("fsync: %s (datasync %s, fh %s)" % (path, datasync, fh))
			self._fflush(fh)
			if datasync and hasattr(os, 'fdatasync'):
				os.fdatasync(fh.fileno())
			else:
				os.fsync(fh.fileno())

	#####################

	class Sha1File(object):
		def __init__(self, path, flags, *mode):
			self.file = os.fdopen(os.open("." + path, flags, *mode), flag2mode(flags))
			#logging.info("%s: %s" % (path, sumfile(self.file)))
			self.fd = self.file.fileno()
			self.path = "." + path
			#logging.info("=============================== %s" % self.opts)

		def read(self, length, offset):
			"""
			Get all or part of the contents of a file.
			length: Size in bytes to read.
			offset: Offset in bytes from the start of the file to read from.
			Does not need to check access rights (operating system will always
			call access or open first).
			Returns a byte string with the contents of the file, with a length no
			greater than 'size'. May also return an int error code.
			
			If the length of the returned string is 0, it indicates the end of the
			file, and the OS will not request any more. If the length is nonzero,
			the OS may request more bytes later.
			To signal that it is NOT the end of file, but no bytes are presently
			available (and it is a non-blocking read), return -errno.EAGAIN.
			If it is a blocking read, just block until ready.
			"""
			logging.debug("read: %s (length %s, offset %s)" % (self.path, length, offset))
			self.file.seek(offset)
			return self.file.read(length)

		def write(self, buf, offset):
			"""
			Write over part of a file.
			buf: Byte string containing the text to write.
			offset: Offset in bytes from the start of the file to write to.
			Does not need to check access rights (operating system will always
			call access or open first).
			Should only overwrite the part of the file from offset to
			offset+len(buf).
			
			Must return an int: the number of bytes successfully written (should
			be equal to len(buf) unless an error occured). May also be a negative
			int, which is an errno code.
			"""
			logging.debug("write: %s (offset %s)" % (self.path, offset))
			logging.debug("  buf: %r" % buf)
			self.file.seek(offset)
			self.file.write(buf)
			return len(buf)

		def release(self, flags):
			"""
			Closes an open file. Allows filesystem to clean up.
			mode: The same flags the file was opened with (see open).
			"""
			logging.debug("release: %s (flags %s)" % (self.path, oct(flags)))
			self.file.close()

		def _fflush(self):
			if 'w' in self.file.mode or 'a' in self.file.mode:
				self.file.flush()

		def fsync(self, isfsyncfile):
			"""
			Synchronises an open file.
			isfsyncfile: If True, only flush user data, not metadata.
			"""
			logging.debug("fsync: %s (isfsyncfile %s)" % (self.path, isfsyncfile))
			self._fflush()
			if isfsyncfile and hasattr(os, 'fdatasync'):
				os.fdatasync(self.fd)
			else:
				os.fsync(self.fd)

		def flush(self):
			"""
			Flush cached data to the file system.
			This is NOT an fsync (I think the difference is fsync goes both ways,
			while flush is just one-way).
			"""
			logging.debug("flush: %s" % self.path)
			self._fflush()
			# cf. xmp_flush() in fusexmp_fh.c
			os.close(os.dup(self.fd))

		def fgetattr(self):
			"""
			Retrieves information about a file (the "stat" of a file).
			Same as Fuse.getattr, but may be given a file handle to an open file,
			so it can use that instead of having to look up the path.
			"""
			logging.debug("fgetattr: %s" % self.path)
			return os.fstat(self.fd)

		def ftruncate(self, length):
			"""
			Shrink or expand a file to a given size.
			Same as Fuse.truncate, but may be given a file handle to an open file,
			so it can use that instead of having to look up the path.
			"""
			logging.debug("ftruncate: %s (size %s)" % (self.path, length))
			self.file.truncate(length)

		def lock(self, cmd, owner, **kw):
			# The code here is much rather just a demonstration of the locking
			# API than something which actually was seen to be useful.

			# Advisory file locking is pretty messy in Unix, and the Python
			# interface to this doesn't make it better.
			# We can't do fcntl(2)/F_GETLK from Python in a platfrom independent
			# way. The following implementation *might* work under Linux.
			#
			# if cmd == fcntl.F_GETLK:
			#     import struct
			#
			#     lockdata = struct.pack('hhQQi', kw['l_type'], os.SEEK_SET,
			#                            kw['l_start'], kw['l_len'], kw['l_pid'])
			#     ld2 = fcntl.fcntl(self.fd, fcntl.F_GETLK, lockdata)
			#     flockfields = ('l_type', 'l_whence', 'l_start', 'l_len', 'l_pid')
			#     uld2 = struct.unpack('hhQQi', ld2)
			#     res = {}
			#     for i in xrange(len(uld2)):
			#          res[flockfields[i]] = uld2[i]
			#
			#     return fuse.Flock(**res)

			# Convert fcntl-ish lock parameters to Python's weird
			# lockf(3)/flock(2) medley locking API...
			op = { fcntl.F_UNLCK : fcntl.LOCK_UN,
						 fcntl.F_RDLCK : fcntl.LOCK_SH,
						 fcntl.F_WRLCK : fcntl.LOCK_EX }[kw['l_type']]
			if cmd == fcntl.F_GETLK:
				return -EOPNOTSUPP
			elif cmd == fcntl.F_SETLK:
				if op != fcntl.LOCK_UN:
					op |= fcntl.LOCK_NB
			elif cmd == fcntl.F_SETLKW:
				pass
			else:
				return -EINVAL

			fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])

#######################

	def main(self, *a, **kw):
		#self.file_class = self.Sha1File
		return Fuse.main(self, *a, **kw)

def main():
	usage = """
Userspace SHA1 checksum FS: mirror the filesystem tree, adding and updating file checksums.

	""" + Fuse.fusage

	server = Sha1FS(version="%prog " + fuse.__version__,
									usage=usage,
									dash_s_do='setsingle')

	server.parser.add_option(mountopt="root", metavar="PATH", default='/',
													 help="mirror filesystem from under PATH [default: %default]")
	server.parse(values=server, errex=1)
	opts, args = server.cmdline
	
	database = opts.database
	
	if database == None:
		server.parser.print_help()
		# how do I make this an arg?
		print "Error: Missing SQLite database argument."
		sys.exit()
		
	# init the database if it does not exist
	dbExists = os.path.exists(database)
	
		
	if not dbExists:
		with sqlite.connect(database) as connection:
			cursor = connection.cursor()
			cursor.execute("""create table if not exists files(
	path varchar not null primary key,
	chksum varchar not null);""")
			cursor.close()
	
			connection.commit
	#with file(opts.database, 'a'):
	#	os.utime(opts.database, None)

	try:
		if server.fuse_args.mount_expected():
			os.chdir(server.root)
	except OSError:
		print >> sys.stderr, "can't enter root of underlying filesystem"
		sys.exit(1)

	server.main()

if __name__ == '__main__':
	main()
