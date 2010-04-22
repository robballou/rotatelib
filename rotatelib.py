"""
Module for assisting in querying the file system and databases for backups to rotate

Currently can parse the following date formats in file names:

- YYYY-MM-DDTHHMM-Z
- YYYYMMDD

Example usage:

>>> import datetime
>>> import rotatelib
>>> rotatelib.list_archives(directory='/backups/', before=datetime.timedelta(1))
...
>>> items = rotatelib.list_archives(directory='/backups/', before=datetime.datetime(2009, 6, 20))
>>> rotatelib.remove_items(items)

Can also handle database connections:

>>> import datetime
>>> import rotatelib
>>> import MySQLdb
>>> db = MySQLdb.connect('localhost', 'user', 'password', 'my_database')
>>> items = rotatelib.list_backup_tables(db=db, before=datetime.timedelta(5))
>>> rotatelib.remove_items(db=db, items)

"""
__author__ = 'Rob Ballou (rob.ballou@gmail.com)'
__version__ = '0.1'
__license__ = 'MIT'

import optparse
import re
import datetime
import os

def is_archive(fn):
    """
    Determines if the requested filename is an archive or not. See parse_name()
    """
    extensions = ['.gz', '.bz2', '.zip', '.tgz']
    if fn[-3:] in extensions or fn[-4:] in extensions:
        return True
    return False

def is_backup_table(table):
    """
    Determines if the table name is an archive or not. See parse_name()
    """
    try:
        parsed = parse_name(table)
    except Exception, e:
        raise Exception('Could not parse the table name <%s>: %s' % (table, e))
    return parsed['date'] != None

def is_log(fn):
    """Determines if the requested filename is an archive or not"""
    extensions = ['.log']
    if fn[-3:] in extensions or fn[-4:] in extensions:
        return True
    return False

def parse_name(fn):
    """
    Figure out if the given filename has a date portion or not. This returns a dictionary with the 
    name of the item and the date, if applicable.
    """
    item = {'name': fn, 'date': None}
    # check YYYY-MM-DDTHHMM-Z
    if re.search(r'(\d{4})-(\d{2})-(\d{2})T(\d{2})(\d{2})?-?(\d{4})?', fn):
        result = re.findall(r'(\d{4})-(\d{2})-(\d{2})T(\d{2})(\d{2})?-?(\d{4})?', fn)[0]
        minute = 0
        if result[4]:
            minute = int(result[4])
        item['date'] = datetime.datetime(int(result[0]), int(result[1]), int(result[2]), int(result[3]), minute)
    # check YYYYMMDD
    elif re.search(r'(\d{4})(\d{2})(\d{2})', fn):
        result = re.findall(r'(\d{4})(\d{2})(\d{2})', fn)[0]
        item['date'] = datetime.datetime(int(result[0]), int(result[1]), int(result[2]))
    return item

def list_archives(directory='./', items=None, **kwargs):
    """
    List all of the archive files in the directory that meet the criteria (see meets_criteria())
    """
    if not items:
        items = os.listdir(directory)
    items = [archive for archive in items if is_archive(archive) and meets_criteria(directory, archive, **kwargs)]
    return items

def list_backup_tables(db, db_type=None, **kwargs):
    """Find backed up tables in the database"""
    cur = db.cursor()
    tables = None
    if db_type == 'mysql' or db_type == None:
        try:
            cur.execute('SHOW TABLES')
            tables = [table[0] for table in cur.fetchall()]
        except Exception, e:
            pass
    if not tables and (db_type in ['sqlite', 'sqlite3'] or db_type == None):
        try:
            cur.execute('''SELECT * FROM sqlite_master WHERE type='table' ''')
            tables = [table[1] for table in cur.fetchall()]
        except Exception, e:
            raise e
    
    if not tables: raise Exception('Could not figure out the database type or get a list of tables')
    
    backup_tables = [table for table in tables if is_backup_table(table) and meets_criteria(db, table, **kwargs)]
    return backup_tables

def list_logs(directory='./', items=None, **kwargs):
    """
    List all of the log files in the directory that meet the criteria (see meets_criteria())
    """
    if not items:
        items = os.listdir(directory)
    items = [archive for archive in items if is_log(archive) and meets_criteria(directory, archive, **kwargs)]
    return items

def meets_criteria(directory, filename, **kwargs):
    """
    Check the filename to see if it meets the criteria for this query
    
    Current criteria:
        after
        before
        except_hour
        has_date
        hour
        pattern (regex)
    """
    name = parse_name(filename)
    if ((kwargs.has_key('has_date') and kwargs['has_date'] == True) or not kwargs.has_key('has_date')) and not name['date']:
        return False
    if kwargs.has_key('pattern'):
        if not re.match(kwargs['pattern'], filename):
            return False
    if name['date']:
        if kwargs.has_key('before'):
            # check if this is a timedelta object
            try:
                if kwargs['before'].days:
                    kwargs['before'] = datetime.datetime.today() - kwargs['before']
            except AttributeError, e:
                pass
            if name['date'] > kwargs['before']:
                return False
        if kwargs.has_key('after'):
            try:
                if kwargs['after'].days:
                    kwargs['after'] = datetime.datetime.today() - kwargs['after']
            except AttributeError, e:
                pass
            if name['date'] < kwargs['after']:
                return False
        if kwargs.has_key('hour'):
            # ignore any hour besides the requested one
            if name['date'].hour != kwargs['hour']:
                return False
        if kwargs.has_key('except_hour'):
            # ignore the specified hour
            if name['date'].hour == kwargs['except_hour']:
                return False
    return True

def remove_items(directory='./', items=None, db=None):
    """
    Delete the items in the directory/items list
    """
    if not items: return
    if not db:
        for item in items:
            this_item = os.path.join(directory, item)
            os.remove(this_item)
    else:
        cur = db.cursor()
        for item in items:
            try:
                cur.execute("DROP TABLE %s" % item)
            except Exception, e:
                pass