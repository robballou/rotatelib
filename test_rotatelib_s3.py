"""
New S3 rotatelib test file

NOTE: This requires an AWS account and will cost you some $ to run!

It creates a test bucket, adds, and removes items from that bucket. The bucket will
be removed when we're done.

"""

import unittest
import rotatelib
import datetime
import sqlite3
from boto.s3.connection import S3Connection
from boto.s3.key import Key

TEST_BUCKET_NAME = 'rotatelib_test'

class S3TestCase(unittest.TestCase):
    def setUp(self):
        """Setup our S3 test bucket"""
        self.s3 = S3Connection()
        self.s3.create_bucket(TEST_BUCKET_NAME)
        
        bucket = self.s3.get_bucket(TEST_BUCKET_NAME)
        
        k = Key(bucket)
        k.key = '/folder1/file.txt.bz2'
        k.set_contents_from_filename('test_bucket/folder1/file.txt.bz2')
        
        k2 = Key(bucket)
        k2.key = '/folder1/backup20110420.sql.bz2'
        k2.set_contents_from_filename('test_bucket/folder1/backup20110420.sql.bz2')
        
        k3 = Key(bucket)
        k3.key = '/folder2/file2.txt.bz2'
        k3.set_contents_from_filename('test_bucket/folder2/file2.txt.bz2')
        
        k4 = Key(bucket)
        k4.key = '/folder3/file3.bz2'
        k4.set_contents_from_filename('test_bucket/folder3/file3.bz2')
    
    def tearDown(self):
        """Destroy our test bucket"""
        bucket = self.s3.get_bucket(TEST_BUCKET_NAME)
        items = bucket.list()
        for item in items:
            bucket.delete_key(item)
        self.s3.delete_bucket(TEST_BUCKET_NAME)

class TestArchiveFunctionsWithS3Keys(S3TestCase):
    def testIsArchiveReturnsFalse(self):
        bucket = self.s3.get_bucket(TEST_BUCKET_NAME)
        k = Key(bucket)
        k.key = '/hello.txt'
        k.set_contents_from_string('Hello World!')
        self.assertFalse(rotatelib.is_archive(k))
    
    def testIsArchiveReturnsTrue(self):
        bucket = self.s3.get_bucket(TEST_BUCKET_NAME)
        k = Key(bucket)
        k.key = '/folder1/file.txt.bz2'
        self.assertTrue(rotatelib.is_archive(k))

class TestRotatelibFunctionsWithS3(S3TestCase):
    def testListArchives(self):
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME)
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME, has_date=False)
        self.assertEqual(len(archives), 4)
    
    def testListArchivesWithSingleDirectory(self):
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME, directory='folder1')
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME, directory=['folder1'])
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME, has_date=False, directory='folder1')
        self.assertEqual(len(archives), 2)
    
    def testListArchivesWithMultipleDirectories(self):
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME, directory=['folder1', 'folder2'])
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME, has_date=False, directory=['folder1', 'folder3'])
        self.assertEqual(len(archives), 3)
    
    def testRemoveItems(self):
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME)
        rotatelib.remove_items(items=archives, s3bucket=TEST_BUCKET_NAME)
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME)
        self.assertEqual(len(archives), 0)
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME, has_date=False)
        self.assertEqual(len(archives), 3)

if __name__ == "__main__":
    unittest.main()