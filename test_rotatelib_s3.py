"""
New S3 rotatelib test file

***********************************************************************
WARNING
This requires an AWS account and will cost you some $ to run!
***********************************************************************

It creates a test bucket, adds, and removes items from that bucket. The
bucket will be removed when we're done.

"""
import argparse
import unittest
import rotatelib
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.ec2.connection import EC2Connection
from boto.exception import EC2ResponseError

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
        self.assertEqual(len(archives), 2)

    def testRemoveItems(self):
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME)
        rotatelib.remove_items(items=archives, s3bucket=TEST_BUCKET_NAME)
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME)
        self.assertEqual(len(archives), 0)
        archives = rotatelib.list_archives(s3bucket=TEST_BUCKET_NAME, has_date=False)
        self.assertEqual(len(archives), 1)


class EC2TestCase(unittest.TestCase):
    def setUp(self):
        """Setup our EC2 instance"""
        self.ec2 = EC2Connection()
        self.volume = self.ec2.create_volume(1, 'us-east-1a')
        self.snapshots = []
        self.snapshots.append(self.volume.create_snapshot('rotatelib_backup20121110'))
        self.snapshots.append(self.volume.create_snapshot('rotatelib_backup'))

    def tearDown(self):
        """Destroy our test volume and snapshots"""
        self.volume.delete()
        for snap in self.snapshots:
            try:
                snap.delete()
            except EC2ResponseError:
                continue


class TestRotatelibFunctionsWithEC2(EC2TestCase):
    def testListArchives(self):
        archives = rotatelib.list_archives(ec2snapshots=True, startswith="rotatelib")
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(ec2snapshots=True, startswith="rotatelib", has_date=False)
        self.assertEqual(len(archives), 2)

    def testRemoveItems(self):
        archives = rotatelib.list_archives(ec2snapshots=True, startswith="rotatelib")
        self.assertEqual(len(archives), 1)
        rotatelib.remove_items(items=archives, ec2snapshots=True)
        archives = rotatelib.list_archives(ec2snapshots=True, startswith="rotatelib")
        self.assertEqual(len(archives), 0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--ec2', dest='ec2', action='store_true', default=False)
    parser.add_argument('--s3', dest='s3', action='store_true', default=False)
    args = parser.parse_args()

    if (not args.ec2 and not args.s3) or (args.ec2 and args.s3):
        unittest.main()
    else:
        suite = unittest.TestSuite()
        test_loader = unittest.TestLoader()
        if args.ec2 and not args.s3:
            tests = test_loader.loadTestsFromTestCase(TestRotatelibFunctionsWithEC2)
            suite.addTests(tests)
        elif args.s3 and not args.ec2:
            suite.addTests(test_loader.loadTestsFromTestCase(TestArchiveFunctionsWithS3Keys))
            suite.addTests(test_loader.loadTestsFromTestCase(TestRotatelibFunctionsWithS3))
        unittest.TextTestRunner(verbosity=2).run(suite)
