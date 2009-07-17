import unittest
import rotatelib
import datetime

class TestArchiveFunctions(unittest.TestCase):
    def testIsArchiveReturnsFalse(self):
        files = ['test.txt', '.test', 'something.sql']
        for f in files:
            self.assertFalse(rotatelib.is_archive(f))
    
    def testIsArchiveReturnsTrue(self):
        files = ['test.gz', 'test.bz2', 'something.sql.bz2', 'test.zip']
        for f in files:
            self.assertTrue(rotatelib.is_archive(f))
    
    def testParseName1(self):
        f = 'test-2009-06-29T1430-0700.bz2'
        result = rotatelib.parse_name(f)
        self.assertTrue(result['date'])
        self.assertEqual(result['date'].year, 2009)
        self.assertEqual(result['date'].month, 6)
    
    def testParseName2(self):
        f = 'test-2009-06-29T14-0700.bz2'
        result = rotatelib.parse_name(f)
        self.assertTrue(result['date'])
        self.assertEqual(result['date'].year, 2009)
        self.assertEqual(result['date'].month, 6)
    
    def testParseName3(self):
        f = 'test-20090629.bz2'
        result = rotatelib.parse_name(f)
        self.assertTrue(result['date'])
        self.assertEqual(result['date'].year, 2009)
        self.assertEqual(result['date'].month, 6)

class TestRotationFunctions(unittest.TestCase):
    def testListArchive(self):
        items = ['test.txt', 'test.zip']
        archives = rotatelib.list_archives(items=items, has_date=False)
        self.assertEqual(len(archives), 1)
    
    def testListArchiveWithBeforeCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        archives = rotatelib.list_archives(items=items, after=datetime.datetime(2009, 6, 20))
        self.assertEqual(len(archives), 1)
    
    def testMeetsCriteriaWithBefore(self):
        files = ['file2009-06-20T15.sql.bz2', 'file2009-06-25T15.sql.bz2']
        self.assertTrue(rotatelib.meets_criteria('./', files[0], before=datetime.datetime.today()))
        self.assertFalse(rotatelib.meets_criteria('./', files[1], before=datetime.datetime(2009, 6, 23)))
    
    def testMeetsCriteriaWithBeforeDelta(self):
        files = ['file2009-06-20T15.sql.bz2', 'file2009-06-25T15.sql.bz2']
        self.assertTrue(rotatelib.meets_criteria('./', files[0], before=datetime.timedelta(1)))
        self.assertFalse(rotatelib.meets_criteria('./', files[1], before=datetime.datetime(2009, 6, 23)))
    
    def testMeetsCriteriaWithAfter(self):
        files = ['file2009-06-20T15.sql.bz2', 'file2009-06-25T15.sql.bz2']
        self.assertFalse(rotatelib.meets_criteria('./', files[0], after=datetime.datetime.today()))
        self.assertTrue(rotatelib.meets_criteria('./', files[1], after=datetime.datetime(2009, 6, 23)))
    
    def testListArchiveWithHourCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), hour=12)
        self.assertEqual(len(archives), 0)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), hour=11)
        self.assertEqual(len(archives), 1)
    

if __name__ == "__main__":
    unittest.main()