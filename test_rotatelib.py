import unittest
import rotatelib
import datetime
import sqlite3


class SnapshotMock(object):
    def __init__(self, description=None, start_time=None):
        self.description = description
        self.start_time = start_time


class TestArchiveFunctions(unittest.TestCase):
    def testIsArchiveReturnsFalse(self):
        files = ['test.txt', '.test', 'something.sql', 'something.min.js']
        for f in files:
            self.assertFalse(rotatelib.is_archive(f))

    def testIsArchiveReturnsTrue(self):
        files = ['test.gz', 'test.bz2', 'something.sql.bz2', 'test.zip', 'test.sql.gz']
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

    def testParseName4(self):
        f = 'Screenshot 2017-02-27 12.51.24.png'
        result = rotatelib.parse_name(f)
        self.assertTrue(result['date'])
        self.assertEqual(result['date'].year, 2017)
        self.assertEqual(result['date'].month, 2)
        self.assertEqual(result['date'].day, 27)

    def testParseNameSnapshotObject(self):
        o = SnapshotMock('Test 2011-01-01')
        result = rotatelib.parse_name(o)
        self.assertTrue(result['date'])
        self.assertEqual(result['date'].year, 2011)
        self.assertEqual(result['date'].month, 1)

        o = SnapshotMock('Test 2011-01-01', '2011-01-01T01:30:00.000Z')
        result = rotatelib.parse_name(o)
        self.assertTrue(result['date'])
        self.assertEqual(result['date'].year, 2011)
        self.assertEqual(result['date'].month, 1)
        self.assertEqual(result['date'].hour, 0)

        o = SnapshotMock('Test', '2011-01-01T01:30:00.000Z')
        result = rotatelib.parse_name(o)
        self.assertTrue(result['date'])
        self.assertEqual(result['date'].year, 2011)
        self.assertEqual(result['date'].month, 1)
        self.assertEqual(result['date'].hour, 1)
        self.assertEqual(result['date'].minute, 30)

        # check that snapshot_use_start_time prefers the start_time
        o = SnapshotMock('Test 2011-02-01', '2011-01-01T01:30:00.000Z')
        result = rotatelib.parse_name(o, snapshot_use_start_time=True)
        self.assertTrue(result['date'])
        self.assertEqual(result['date'].year, 2011)
        self.assertEqual(result['date'].month, 1)
        self.assertEqual(result['date'].hour, 1)
        self.assertEqual(result['date'].minute, 30)

        # check that prefers description
        o = SnapshotMock('Test 2011-02-01', '2011-01-01T01:30:00.000Z')
        result = rotatelib.parse_name(o, snapshot_use_start_time=False)
        self.assertTrue(result['date'])
        self.assertEqual(result['date'].year, 2011)
        self.assertEqual(result['date'].month, 2)
        self.assertEqual(result['date'].hour, 0)

    def testMakeList(self):
        self.assertEqual([1], rotatelib._make_list(1))


class TestRotationFunctions(unittest.TestCase):
    def testListArchive(self):
        items = ['test.txt', 'test.zip']
        archives = rotatelib.list_archives(items=items, has_date=False)
        self.assertEqual(len(archives), 1)  # this should only return test.zip

    def testMultiplePatterns(self):
        self.assertTrue(rotatelib.meets_criteria('./', 'test20121110.zip', before=datetime.datetime(2013, 1, 1)))

    def testListArchiveWithHasDateCriteria(self):
        items = ['test.zip', 'test20121110.zip']
        # this is the same as has_date = True
        archives = rotatelib.list_archives(items=items)
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(items=items, has_date=True)
        self.assertEqual(len(archives), 1)
        # set has_date to false to get items without their date
        archives = rotatelib.list_archives(items=items, has_date=False)
        self.assertEqual(len(archives), 2)

    def testMeetsCriteriaHasDate(self):
        items = ['test.zip', 'test20121110.zip']
        self.assertFalse(rotatelib.meets_criteria('./', items[0]))
        self.assertFalse(rotatelib.meets_criteria('./', items[0], has_date=True))
        self.assertTrue(rotatelib.meets_criteria('./', items[0], has_date=False))
        self.assertTrue(rotatelib.meets_criteria('./', items[1], has_date=True))

    def testMeetsCriteriaPattern(self):
        items = ['test.zip', 'test20121110.zip']
        self.assertTrue(rotatelib.meets_criteria('./', items[1], pattern=r'^test'))
        self.assertFalse(rotatelib.meets_criteria('./', items[1], pattern=r'^steve'))

    def testMeetsCriteriaWithStartswith(self):
        items = ['test.zip', 'test20121110.zip']
        self.assertTrue(rotatelib.meets_criteria('./', items[1], startswith='test'))
        self.assertTrue(rotatelib.meets_criteria('./', items[1], startswith=['test', 'steve']))
        self.assertTrue(rotatelib.meets_criteria('./', items[1], startswith=['steve', 'test']))
        self.assertFalse(rotatelib.meets_criteria('./', items[1], startswith='steve'))
        self.assertFalse(rotatelib.meets_criteria('./', items[1], startswith=['steve']))

    def testMeetsCriteriaWithExceptStartswith(self):
        items = ['test.zip', 'test20121110.zip']
        self.assertFalse(rotatelib.meets_criteria('./', items[1], except_startswith='test'))
        self.assertFalse(rotatelib.meets_criteria('./', items[1], except_startswith=['test', 'steve']))
        self.assertFalse(rotatelib.meets_criteria('./', items[1], except_startswith=['steve', 'test']))
        self.assertTrue(rotatelib.meets_criteria('./', items[1], except_startswith='steve'))
        self.assertTrue(rotatelib.meets_criteria('./', items[1], except_startswith=['steve']))

    def testListArchiveWithBeforeCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20))
        self.assertEqual(len(archives), 1)

    def testMeetsCriteriaWithBefore(self):
        files = ['file2009-06-20T15.sql.bz2', 'file2009-06-25T15.sql.bz2', 'file2009-06-25.sql.bz2']
        self.assertTrue(rotatelib.meets_criteria('./', files[0], before=datetime.datetime.today()))
        self.assertFalse(rotatelib.meets_criteria('./', files[1], before=datetime.datetime(2009, 6, 23)))
        self.assertFalse(rotatelib.meets_criteria('./', files[1], before=datetime.datetime(2009, 6, 25)))
        self.assertFalse(rotatelib.meets_criteria('./', files[2], before=datetime.datetime(2009, 6, 25)))

    def testMeetsCriteriaWithBeforeDelta(self):
        files = ['file2009-06-20T15.sql.bz2', 'file2009-06-25T15.sql.bz2']
        self.assertTrue(rotatelib.meets_criteria('./', files[0], before=datetime.timedelta(1)))
        self.assertFalse(rotatelib.meets_criteria('./', files[1], before=datetime.datetime(2009, 6, 23)))

    def testMeetsCriteriaWithAfter(self):
        files = ['file2009-06-20T15.sql.bz2', 'file2009-06-25T15.sql.bz2', 'file2009-06-25.sql.bz2']
        self.assertFalse(rotatelib.meets_criteria('./', files[0], after=datetime.datetime.today()))
        self.assertTrue(rotatelib.meets_criteria('./', files[1], after=datetime.datetime(2009, 6, 23)))
        self.assertFalse(rotatelib.meets_criteria('./', files[2], after=datetime.datetime(2009, 6, 25)))

    def testListArchiveWithHourCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), hour=12)
        self.assertEqual(len(archives), 0)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), hour=11)
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), hour=[11, 12])
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), hour=[13, 12])
        self.assertEqual(len(archives), 0)

    def testListArchiveWithExceptHourCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        # sanity check
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 21))
        self.assertEqual(len(archives), 2)

        # ignore 11
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 21), except_hour=11)
        self.assertEqual(len(archives), 1)

        # ignore 12 (doesn't match anything)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 21), except_hour=12)
        self.assertEqual(len(archives), 2)

        # ignore 11 and 12
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 21), except_hour=[11, 12])
        self.assertEqual(len(archives), 1)

        # ignore 13 and 12
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 21), except_hour=[13, 12])
        self.assertEqual(len(archives), 2)

    def testMeetsCriteriaWithDayCriteria(self):
        self.assertFalse(rotatelib.meets_criteria('./', 'test.zip', day=1))
        self.assertTrue(rotatelib.meets_criteria('./', 'test20120101.zip', day=1))
        self.assertTrue(rotatelib.meets_criteria('./', 'test20120101.zip', day=[1]))
        self.assertTrue(rotatelib.meets_criteria('./', 'test2009-06-15T11.zip', before=datetime.datetime(2009, 6, 20), day=[15]))

    def testListArchiveWithDayCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), day=1)
        self.assertEqual(len(archives), 0)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), day=15)
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), day=[1, 15])
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), day=[2, 4])
        self.assertEqual(len(archives), 0)

    def testListArchiveWithExceptDayCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        # before criteria will only yield a pass for test test2009-06-15T11.zip, but except_day should cause that to fail
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), except_day=15)
        self.assertEqual(len(archives), 0)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), except_day=1)
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), except_day=[1, 15])
        self.assertEqual(len(archives), 0)
        archives = rotatelib.list_archives(items=items, before=datetime.datetime(2009, 6, 20), except_day=[1, 12])
        self.assertEqual(len(archives), 1)

    def testListArchiveWithStartswithCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        archives = rotatelib.list_archives(items=items, startswith='test')
        self.assertEqual(len(archives), 2)
        archives = rotatelib.list_archives(items=items, startswith=['test', 'asdf'])
        self.assertEqual(len(archives), 2)
        archives = rotatelib.list_archives(items=items, startswith='asdf')
        self.assertEqual(len(archives), 0)
        archives = rotatelib.list_archives(items=items, startswith='test', before=datetime.datetime(2009, 6, 21))
        self.assertEqual(len(archives), 2)
        archives = rotatelib.list_archives(items=items, startswith='test', before=datetime.datetime(2009, 6, 20))
        self.assertEqual(len(archives), 1)

    def testListArchiveWithExceptStartswithCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        archives = rotatelib.list_archives(items=items, except_startswith='test')
        self.assertEqual(len(archives), 0)
        archives = rotatelib.list_archives(items=items, except_startswith='asdf')
        self.assertEqual(len(archives), 2)

    def testListArchiveWithEndswithCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        archives = rotatelib.list_archives(items=items, endswith='bz2')
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(items=items, endswith=['bz2', 'zip'])
        self.assertEqual(len(archives), 2)
        archives = rotatelib.list_archives(items=items, endswith='asdf')
        self.assertEqual(len(archives), 0)

    def testListArchiveWithExceptEndswithCriteria(self):
        items = ['test.txt', 'test2009-06-15T11.zip', 'test2009-06-20T01.bz2', 'test.zip']
        archives = rotatelib.list_archives(items=items, except_endswith='bz2')
        self.assertEqual(len(archives), 1)
        archives = rotatelib.list_archives(items=items, except_endswith='asdf')
        self.assertEqual(len(archives), 2)

    def testMeetsCriteriaWithYearCriteria(self):
        self.assertTrue(rotatelib.meets_criteria("./", "test20120101.zip", year=2012))
        self.assertFalse(rotatelib.meets_criteria("./", "test20110101.zip", year=2012))
        self.assertTrue(rotatelib.meets_criteria("./", "test20110101.zip", year=[2012, 2011]))

    def testMeetsCriteriaWithExceptYearCriteria(self):
        self.assertFalse(rotatelib.meets_criteria("./", "test20120101.zip", except_year=2012))
        self.assertTrue(rotatelib.meets_criteria("./", "test20110101.zip", except_year=2012))

    def testListItemsWithExceptFirstPerDayCriteria(self):
        items = ['test2014-05-20T013000.sql', 'test2014-05-20T023000.sql', 'test2014-06-20T013000.sql']
        found_items = rotatelib.list_items(items=items, except_first='day')
        self.assertEqual(len(found_items), 1)
        self.assertFalse('test2014-05-20T013000.sql' in found_items)
        self.assertFalse('test2014-06-20T013000.sql' in found_items)

    def testListItemsWithExceptFirstPerMonthCriteria(self):
        items = ['test2014-05-20T013000.sql', 'test2014-05-20T023000.sql', 'test2014-06-20T013000.sql']
        found_items = rotatelib.list_items(items=items, except_first='month')
        self.assertEqual(len(found_items), 1)
        self.assertFalse('test2014-05-20T013000.sql' in found_items)
        self.assertFalse('test2014-06-20T013000.sql' in found_items)

    def testListItemsWithExceptLastPerDayCriteria(self):
        items = [
          'test2014-05-20T013000.sql',
          'test2014-05-20T023000.sql',
          'test2014-06-20T013000.sql'
        ]
        found_items = rotatelib.list_items(items=items, except_last='day')
        self.assertEqual(len(found_items), 1)
        self.assertTrue('test2014-05-20T013000.sql' in found_items)

    def testListItemsWithExceptLastPerMonthCriteria(self):
        items = ['test2014-05-20T013000.sql', 'test2014-05-20T023000.sql', 'test2014-06-20T013000.sql']
        found_items = rotatelib.list_items(items=items, except_last='month')
        self.assertEqual(len(found_items), 1)
        self.assertFalse('test2014-05-20T023000.sql' in found_items)
        self.assertFalse('test2014-06-20T013000.sql' in found_items)
        self.assertTrue('test2014-05-20T013000.sql' in found_items)


class TestDBRotationFunctions(unittest.TestCase):
    def create_tables(self, db, tables):
        cur = db.cursor()
        for table in tables:
            cur.execute('CREATE TABLE %s (value VARCHAR(50) NOT NULL)' % table)
        db.commit()

    def testIsBackupTable(self):
        # not backup tables
        tables = ['tableA', 'tableB']
        for table in tables:
            self.assertFalse(rotatelib.is_backup_table(table))

        # backup tables
        tables = ['tableA20090922T162037', 'tableA20090922']
        for table in tables:
            self.assertTrue(rotatelib.is_backup_table(table))

    def testCanDetectTables(self):
        tables = ['tableA', 'tableB', 'tableA20090922T162037', 'tableA20090922']
        db = sqlite3.connect(':memory:')
        self.create_tables(db, tables)
        tables = rotatelib.list_backup_tables(db)
        self.assertEqual(len(tables), 2)
        self.assertEqual(tables[0], 'tableA20090922T162037')
        self.assertEqual(tables[1], 'tableA20090922')

    def testMeetsCriteriaWithBeforeDelta(self):
        tables = ['tableA', 'tableB', 'tableA20090922T162037', 'table%s' % datetime.date.today().strftime("%Y%m%d")]
        db = sqlite3.connect(':memory:')
        self.create_tables(db, tables)
        tables = rotatelib.list_backup_tables(db, before=datetime.timedelta(1))
        self.assertEqual(len(tables), 1)

    def testRemoveTablesWithCriteria(self):
        tables = ['tableA', 'tableB', 'tableA20090922T162037', 'table%s' % datetime.date.today().strftime("%Y%m%d")]
        db = sqlite3.connect(':memory:')
        self.create_tables(db, tables)
        tables = rotatelib.list_backup_tables(db, before=datetime.timedelta(1))
        self.assertEqual(len(tables), 1)
        rotatelib.remove_items(db=db, items=tables)
        tables = rotatelib.list_backup_tables(db, before=datetime.timedelta(1))
        self.assertEqual(len(tables), 0)
        cur = db.cursor()
        cur.execute("SELECT * FROM sqlite_master WHERE type='table'")
        tables = cur.fetchall()
        self.assertEqual(len(tables), 3)

    def testRemoveTablesWithPatternCriteria(self):
        tables = ['tableA', 'tableB', 'tableA20090922T162037', 'steveA20090922T162037', 'table%s' % datetime.date.today().strftime("%Y%m%d")]
        db = sqlite3.connect(':memory:')
        self.create_tables(db, tables)
        tables = rotatelib.list_backup_tables(db, before=datetime.timedelta(1), pattern='steve.*')
        self.assertEqual(len(tables), 1)
        rotatelib.remove_items(db=db, items=tables)
        tables = rotatelib.list_backup_tables(db, before=datetime.timedelta(1))
        self.assertEqual(len(tables), 1)
        cur = db.cursor()
        cur.execute("SELECT * FROM sqlite_master WHERE type='table'")
        tables = cur.fetchall()
        self.assertEqual(len(tables), 4)


if __name__ == "__main__":
    unittest.main()
