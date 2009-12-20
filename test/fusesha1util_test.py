import unittest
import sys
sys.path.append("../")
import os

import fusesha1util as fsu

class TestSha1FuseUtil(unittest.TestCase):
	# Test the variations on sha1sum
	def testSha1Sum(self):
		self.assertEqual("9519b846c2b3a933bd348cc983f3796180ad2761", fsu.sha1sum("sha1test.txt"))
		self.assertRaises(IOError, lambda: fsu.sha1sum(""))
		self.assertRaises(IOError, lambda: fsu.sha1sum(None))
		
	def testSafeMakeDirs(self):
		parent = "testdirnoexist"
		subdir = os.path.join(parent, "somesubdir")
		subfile = os.path.join(subdir, "file.txt")
		if os.path.exists(subdir): os.removedirs(subdir)
		self.assertFalse(os.path.exists(parent))
		self.assertFalse(os.path.exists(subdir))
		self.assertEqual(subdir, fsu.safeMakedirs(subfile))
		# run again to make sure it doesn't choke
		self.assertEqual(subdir, fsu.safeMakedirs(subfile))
		self.assertTrue(os.path.exists(parent))
		self.assertTrue(os.path.exists(subdir))
		if os.path.exists(subdir): os.removedirs(subdir)
		
	def testSafeMakeDirsBad(self):
		self.assertRaises(OSError, lambda: fsu.safeMakedirs(""))
		self.assertRaises(OSError, lambda: fsu.safeMakedirs(None))
		
if __name__ == '__main__':
	unittest.main()
