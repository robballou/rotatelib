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

And now S3 connections, if you have the boto library installed:

>>> import datetime
>>> import rotatelib
>>> rotatelib.list_archives(s3bucket='mybucket', directory='/backups/', before=datetime.timedelta(1))
...
>>> items = rotatelib.list_archives(s3bucket='mybucket', directory='/backups/', before=datetime.datetime(2009, 6, 20))
>>> rotatelib.remove_items(items=items, s3bucket='mybucket')

For S3 connections to work, you'll need the AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID variables set in your
environment or you'll need to pass those as keyword arguments to list_archives or remove_items.

"""
__author__ = 'Rob Ballou (rob.ballou@gmail.com)'
__version__ = '0.6'
__license__ = 'MIT'

import collections
import optparse
import re
import datetime
import os

try:
    from boto.s3.connection import S3Connection
    from boto.ec2.connection import EC2Connection
    from boto.ec2.snapshot import Snapshot
except ImportError, e:
    pass

def connect_to_ec2(aws_access_key_id, aws_secret_access_key):
    """
    Connect to the ec2 account
    
    Using the boto library, we'll connect to the S3 account. If aws_access_key_id and
    aws_secret_access_key are None, we'll check out the environment variables. If no
    authentication information is found, you'll get an Exception.
    """
    if not aws_secret_access_key and not os.environ['AWS_SECRET_ACCESS_KEY']:
        raise Exception('The AWS_SECRET_ACCESS_KEY was not set. Either set this environment variable or pass it as aws_secret_access_key')
    if not aws_access_key_id and not os.environ['AWS_ACCESS_KEY_ID']:
        raise Exception('The AWS_ACCESS_KEY_ID was not set. Either set this environment variable or pass it as aws_access_key_id')
    if not aws_access_key_id:
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    if not aws_secret_access_key:
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    return EC2Connection(aws_access_key_id, aws_secret_access_key)

def connect_to_s3(aws_access_key_id, aws_secret_access_key):
    """
    Connect to the S3 account
    
    Using the boto library, we'll connect to the S3 account. If aws_access_key_id and
    aws_secret_access_key are None, we'll check out the environment variables. If no
    authentication information is found, you'll get an Exception.
    """
    if not aws_secret_access_key and not os.environ['AWS_SECRET_ACCESS_KEY']:
        raise Exception('The AWS_SECRET_ACCESS_KEY was not set. Either set this environment variable or pass it as aws_secret_access_key')
    if not aws_access_key_id and not os.environ['AWS_ACCESS_KEY_ID']:
        raise Exception('The AWS_ACCESS_KEY_ID was not set. Either set this environment variable or pass it as aws_access_key_id')
    if not aws_access_key_id:
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    if not aws_secret_access_key:
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    return S3Connection(aws_access_key_id, aws_secret_access_key)

def is_archive(fn):
    """
    Determines if the requested filename is an archive or not. See parse_name()
    
    Returns True/False
    """
    # check if this is an ec2 object
    if isinstance(fn, Snapshot):
        return True
    
    extensions = ['.gz', '.bz2', '.zip', '.tgz']
    try:
        fn = fn.key
    except:
        pass
    basename, extension = os.path.splitext(fn)
    if extension in extensions:
        return True
    return False

def is_backup_table(table):
    """
    Determines if the table name is an archive or not. See parse_name()
    
    Returns True/False
    """
    try:
        parsed = parse_name(table)
    except Exception, e:
        raise Exception('Could not parse the table name <%s>: %s' % (table, e))
    return parsed['date'] != None

def is_log(fn):
    """
    Determines if the requested filename is an archive or not
    
    Returns True/False
    """
    extensions = ['.log']
    basename, extension = os.path.splitext(fn)
    if extension in extensions:
        return True
    return False

def list_archives(directory='./', items=None, s3bucket=None, ec2snapshots=None, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    List all of the archive files in the directory that meet the criteria (see meets_criteria()). This also
    supports S3 connections (see connect_to_s3()).
    
    If `directory` is used without `s3bucket`, then we'll use that as the directory to search for
    archives.
    
    If `s3bucket` is used, we'll connect to the S3 account/bucket to look for items. If used in
    conjuction with `directory`, that will be used as the file prefix.
    
    See meets_criteria() for list of kwargs that can be used to limit the results.
    """
    
    if not items:
        # what type of connection/filesystem are we using?
        if not s3bucket and not ec2snapshots:
            # regular file system request
            directory = _make_list(directory)
            items = []
            for d in directory:
                items.extend(os.listdir(d))
        elif s3bucket and not ec2snapshots:
            # s3 request
            try:
                s3 = connect_to_s3(aws_access_key_id, aws_secret_access_key)
                bucket = s3.get_bucket(s3bucket)
                items = []
                directory = _make_list(directory)
                for d in directory:
                    if d == './': d = ''
                    items.extend([item for item in bucket.list(d)])
            except NameError, e:
                raise Exception('To use the S3 library, you must have the boto python library: %s' % s)
        elif ec2snapshots and not s3bucket:
            # ec2 request
            try:
                ec2 = connect_to_ec2(aws_access_key_id, aws_secret_access_key)
                items = ec2.get_all_snapshots(owner='self')
            except NameError, e:
                raise Exception('To use the EC2 library, you must have the boto python library: %s' % e)
    
    items = [archive for archive in items if is_archive(archive) and meets_criteria(directory, archive, **kwargs)]
    return items

def list_backup_tables(db, db_type=None, **kwargs):
    """
    Find backed up tables in the database
    
    The `db` param should be the database object for the database type. By default we assume that this is
    a MySQL database, but we also support sqlite. To trigger for a different database type, just specify
    the `db_type` argument.
    
    See meets_criteria() for list of kwargs that can be used to limit the results.
    """
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

def list_logs(directory='./', items=None, s3bucket=None, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    List all of the log files in the directory that meet the criteria.
    
    This method is the same as list_archives except that it will only look at things that meet the is_log
    method. This also supports S3 connections (see connect_to_s3()).
    
    See meets_criteria() for list of kwargs that can be used to limit the results.
    """
    if not items:
        if not s3bucket:
            # regular file system request
            directory = _make_list(directory)
            items = []
            for d in directory:
                items.extend(os.listdir(d))
        else:
            # s3 request
            try:
                s3 = connect_to_s3(aws_access_key_id, aws_secret_access_key)
                bucket = s3.get_bucket(s3bucket)
                directory = _make_list(directory)
                items = []
                for d in directory:
                    if d == './': d = ''
                    items.extend([item for item in bucket.list(directory)])
                if debug: print items
            except NameError, e:
                raise Exception('To use the S3 library, you must have the boto python library')
    items = [archive for archive in items if is_log(archive) and meets_criteria(directory, archive, **kwargs)]
    return items

def _make_list(item):
    if isinstance(item, basestring) or not isinstance(item, collections.Iterable):
        item = [item]
    return item

def meets_criteria(directory, filename, **kwargs):
    """
    Check the filename to see if it meets the criteria for this query.
    
    Note: the has_date criteria is "on" by default. So if you don't specify that as "off" then
    this will only pass items that have a date in the filename!
    
    Current criteria:
        after
        before
        day
        exceot_day
        except_hour
        has_date
        hour
        pattern (regex)
        startswith
    
    Other options:
        debug
        snapshot_use_start_time
    """
    # get the filename ... this is a bit "complicated" because we need to handle
    # filenames that might be either EC2 snapshots or S3 items
    try:
        # is this an EC2 snapshot?
        filename = filename.description
    except:
        try:
            # is this an s3 item?
            filename = filename.key
        except:
            pass
    
    # check if we are using the snapshot_use_start_time option
    snapshot_use_start_time = False
    if kwargs.has_key('snapshot_use_start_time'): snapshot_use_start_time = kwargs['snapshot_use_start_time']
    
    # parse the filename
    name = parse_name(filename, snapshot_use_start_time=snapshot_use_start_time)
    
    # check that we parsed a date
    if ((kwargs.has_key('has_date') and kwargs['has_date'] == True) or not kwargs.has_key('has_date')) and not name['date']:
        return False
    # name must match the pattern
    if kwargs.has_key('pattern'):
        if not re.match(kwargs['pattern'], filename):
            return False
    # name must start with the string
    if kwargs.has_key('startswith'):
        startswith = _make_list(kwargs['startswith'])
        passes = False
        for s in startswith:
            if filename.startswith(s):
                passes = True
                break
        if not passes:
            return False
    # name must not start with the string
    if kwargs.has_key('except_startswith'):
        startswith = _make_list(kwargs['except_startswith'])
        passes = False
        for s in startswith:
            if filename.startswith(s):
                passes = True
                break
        if passes:
            return False
    if name['date']:
        if kwargs.has_key('before'):
            # check if this is a timedelta object
            try:
                if kwargs['before'].days:
                    kwargs['before'] = datetime.datetime.today() - kwargs['before']
            except AttributeError, e:
                pass
            if kwargs.has_key('debug'):
                print "Date: %s" % name['date']
                print "Before: %s" % kwargs['before']
            if name['date'] >= kwargs['before']:
                if kwargs.has_key('debug'): print "Failed Before"
                return False
        if kwargs.has_key('after'):
            try:
                if kwargs['after'].days:
                    kwargs['after'] = datetime.datetime.today() - kwargs['after']
            except AttributeError, e:
                pass
            if name['date'] <= kwargs['after']:
                if kwargs.has_key('debug'): print "Failed After"
                return False
        if kwargs.has_key('hour'):
            kwargs['hour'] = _make_list(kwargs['hour'])
            # ignore any hour besides the requested one
            if name['date'].hour not in kwargs['hour']:
                if kwargs.has_key('debug'): print "Failed Hour"
                return False
        if kwargs.has_key('except_hour'):
            kwargs['except_hour'] = _make_list(kwargs['except_hour'])
            # ignore the specified hour
            if name['date'].hour in kwargs['except_hour']:
                if kwargs.has_key('debug'): print "Failed Except Hour"
                return False
        if kwargs.has_key('day'):
            kwargs['day'] = _make_list(kwargs['day'])
            # ignore any day besides the requested on
            if name['date'].day not in kwargs['day']:
                if kwargs.has_key('debug'): print "Failed Day"
                return False
        if kwargs.has_key('except_day'):
            kwargs['except_day'] = _make_list(kwargs['except_day'])
            # ignore any day besides the requested on
            if name['date'].day in kwargs['except_day']:
                if kwargs.has_key('debug'): print "Failed Except Day"
                return False
    return True

def parse_name(fn, debug=False, snapshot_use_start_time=False):
    """
    Figure out if the given filename has a date portion or not. This returns a dictionary with the
    name of the item and the date, if applicable.
    """
    o = None
    try:
        o = fn
        if not snapshot_use_start_time:
            fn = fn.description
        else:
            fn = fn.start_time
    except:
        try:
            fn = fn.key
        except:
            pass
    
    item = {'name': fn, 'date': None}
    # check YYYY-MM-DDTHH:MM:SS
    if re.search(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):?(\d{2}):?(\d{2})?', fn):
        result = re.findall(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):?(\d{2}):?(\d{2})?', fn)[0]
        item['date'] = datetime.datetime(int(result[0]), int(result[1]), int(result[2]), int(result[3]), int(result[4]))
    # check YYYY-MM-DDTHHMM-Z
    elif re.search(r'(\d{4})-(\d{2})-(\d{2})T(\d{2})(\d{2})?-?(\d{4})?', fn):
        result = re.findall(r'(\d{4})-(\d{2})-(\d{2})T(\d{2})(\d{2})?-?(\d{4})?', fn)[0]
        minute = 0
        if result[4]:
            minute = int(result[4])
        item['date'] = datetime.datetime(int(result[0]), int(result[1]), int(result[2]), int(result[3]), minute)
    # check YYYYMMDD
    elif re.search(r'(\d{4})-?(\d{2})-?(\d{2})', fn):
        result = re.findall(r'(\d{4})-?(\d{2})-?(\d{2})', fn)[0]
        item['date'] = datetime.datetime(int(result[0]), int(result[1]), int(result[2]))
    
    if not item['date'] and o:
        try:
            date = o.start_time
            if re.search(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})?', date):
                result = re.findall(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})?', date)[0]
                item['date'] = datetime.datetime(int(result[0]), int(result[1]), int(result[2]), int(result[3]), int(result[4]))
        except:
            pass
    
    if debug:
        print item
    
    return item

def remove_items(directory='./', items=None, db=None, s3bucket=None, ec2snapshots=None, aws_access_key_id=None, aws_secret_access_key=None):
    """
    Delete the items in the directory/items list. See connect_to_s3() for information about using this method
    with S3 accounts.
    """
    if not items: return
    if not db and not s3bucket and not ec2snapshots:
        for item in items:
            this_item = os.path.join(directory, item)
            os.remove(this_item)
    elif not db and s3bucket and not ec2snapshots:
        s3 = connect_to_s3(aws_access_key_id, aws_secret_access_key)
        bucket = s3.get_bucket(s3bucket)
        for item in items:
            bucket.delete_key(item.key)
    elif not db and not s3bucket and ec2snapshots:
        ec2 = connect_to_ec2(aws_access_key_id, aws_secret_access_key)
        for item in items:
            item.delete()
    else:
        cur = db.cursor()
        for item in items:
            try:
                cur.execute("DROP TABLE %s" % item)
            except Exception, e:
                pass