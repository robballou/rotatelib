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
        
        k = Key(bucket)
        k.key = '/folder1/backup20110420.sql.bz2'
        k.set_contents_from_filename('test_bucket/folder1/backup20110420.sql.bz2')
    
    def tearDown(self):
        """Destroy our test bucket"""
        bucket = self.s3.get_bucket(TEST_BUCKET_NAME)
        items = bucket.list()
        for item in items:
            bucket.delete_key(item)
        self.s3.delete_bucket(TEST_BUCKET_NAME)

class TestArchiveFunctionsWithS3Keys(S3TestCase):
    def testIsArchive(self):
        bucket = self.s3.get_bucket(TEST_BUCKET_NAME)
        k = Key(bucket)
        k.key = '/folder1/file.txt.bz2'
        k.set_contents_from_filename('test_bucket/folder1/file.txt.bz2')
        
        self.assertTrue(rotatelib.is_archive(k))

# class TestRotatelibFunctionsWithS3(S3TestCase):
#     def testListArchives(self):
#         archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME)
#         print archives


if __name__ == "__main__":
    unittest.main()