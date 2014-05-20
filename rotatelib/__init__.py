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
__version__ = '1.0rc2'
__license__ = 'MIT'

import collections
import re
import datetime
import os
import criteria
import filters
import inspect

try:
    from boto.s3.connection import S3Connection
    from boto.ec2.connection import EC2Connection
    from boto.ec2.snapshot import Snapshot
except ImportError, e:
    pass

CRITERIA = {
    # 'has_date': criteria.HasDate,
    # 'pattern': criteria.Pattern
}

FILTERS = {}

def add_criteria(class_name):
    CRITERIA.append(class_name)


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


def filter_criteria(items, **kwargs):
    """
    Similar to meets_criteria() but fires afterwards and can filter the entire set (meets_criteria()
    only looks at a single item.
    """
    available_filters = get_filters()

    for argument_filter in kwargs.keys():
        if argument_filter in available_filters:
            this_filter = available_filters[argument_filter]()
            if 'debug' in kwargs and kwargs['debug']:
                this_filter.debugMode = True
            this_filter.set_argument(kwargs[argument_filter])
            items = this_filter.filter(items)
    return items

def get_criteria():
    """
    Get the criteria available for this module
    """
    for item in criteria.__dict__:
        this_item = criteria.__dict__[item]
        if inspect.isclass(this_item) and issubclass(this_item, criteria.BaseCriteria):
            criteria_name = this_item.criteria_name
            if not criteria_name:
                criteria_name = item.lower()
            if criteria_name not in CRITERIA:
                CRITERIA[criteria_name] = this_item
    return CRITERIA


def get_filters():
    for item in filters.__dict__:
      this_item = filters.__dict__[item]
      if inspect.isclass(this_item) and issubclass(this_item, filters.BaseFilter):
          filter_name = this_item.filter_name
          if not filter_name:
              filter_name = item.lower()
          if filter_name not in FILTERS:
              FILTERS[filter_name] = this_item
    return FILTERS

def has_date(fn):
    """
    Does this filename have a date?
    """
    parsed_name = parse_name(fn)
    if parsed_name['date']:
        return True
    return False


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
    Determines if the table name is an archive or not. See parse_name(). Essentially
    the table name must have a date portion.

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

    If `ec2snapshots` is used, we'll connect to AWS account and look for snapshots.

    See meets_criteria() for list of kwargs that can be used to limit the results.
    """

    if not items:
        if not s3bucket and not ec2snapshots:
            # regular file system request
            items = os.listdir(directory)
        elif s3bucket and not ec2snapshots:
            # s3 request
            try:
                s3 = connect_to_s3(aws_access_key_id, aws_secret_access_key)
                bucket = s3.get_bucket(s3bucket)
                if directory == './':
                    directory = ''
                items = [item for item in bucket.list(directory)]
            except NameError, e:
                raise Exception('To use the S3 library, you must have the boto python library: %s' % e)
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

    if not tables:
        raise Exception('Could not figure out the database type or get a list of tables')

    backup_tables = [table for table in tables if is_backup_table(table) and meets_criteria(db, table, **kwargs)]
    return backup_tables


def list_items(directory='./', items=None, s3bucket=None, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    List all of the items that meet the criteria

    This method is very similar to the list_archives and list_logs methods, but allows you to find
    items that are not logs or archives.
    """
    if not items:
        if not s3bucket:
            # regular file system request
            items = os.listdir(directory)
        else:
            # s3 request
            try:
                s3 = connect_to_s3(aws_access_key_id, aws_secret_access_key)
                bucket = s3.get_bucket(s3bucket)
                if directory == './':
                    directory = ''
                items = [item for item in bucket.list(directory)]
            except NameError, e:
                raise Exception('To use the S3 library, you must have the boto python library: %s', e)
    items = [archive for archive in items if has_date(archive) and meets_criteria(directory, archive, **kwargs)]

    filter_items = []
    for item in items:
        filter_items.append({
          'item': item,
          'parsed': parse_name(item)
        })
    items = filter_criteria(filter_items, **kwargs)

    return items


def list_logs(directory='./', items=None, s3bucket=None, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    List all of the log files in the directory that meet the criteria.

    This method is the same as `list_archives` except that it will only look at things that meet the `is_log`
    method. This also supports AWS connections, but only S3 items (EC2Snapshots will likely not end with ".log"
    which is the criteria used for `is_log`)

    See meets_criteria() for list of kwargs that can be used to limit the results.
    """
    if not items:
        if not s3bucket:
            # regular file system request
            items = os.listdir(directory)
        else:
            # s3 request
            try:
                s3 = connect_to_s3(aws_access_key_id, aws_secret_access_key)
                bucket = s3.get_bucket(s3bucket)
                if directory == './':
                    directory = ''
                items = [item for item in bucket.list(directory)]
            except NameError, e:
                raise Exception('To use the S3 library, you must have the boto python library: %s', e)
    items = [archive for archive in items if is_log(archive) and meets_criteria(directory, archive, **kwargs)]
    return items


def _make_list(item):
    if not isinstance(item, collections.Iterable):
        item = [item]
    return item


def meets_criteria(directory, filename, **kwargs):
    """
    Current criteria:

      - after (datetime or timedelta)
      - before (datetime or timedelta)
      - day (int or list of ints)
      - except_day (int or list of ints)
      - except_hour (int or list of ints)
      - except_startswith (string or list of strings)
      - except_year (int or list of ints)
      - has_date (true/false)
      - hour (int or list of ints)
      - startswith (string or list of strings)
      - pattern (regex)
      - year (int or list of ints)
    """
    # figure out the filename
    try:
        filename = filename.description
    except:
        try:
            filename = filename.key
        except:
            pass
    snapshot_use_start_time = False
    if 'snapshot_use_start_time' in kwargs:
        snapshot_use_start_time = kwargs['snapshot_use_start_time']

    # parse the filename
    name = parse_name(filename, snapshot_use_start_time=snapshot_use_start_time)

    # has_date is used by default, so make sure it is on
    if 'has_date' not in kwargs:
        kwargs['has_date'] = True

    # if debug is not there, explicitly set it off
    if 'debug' not in kwargs:
        kwargs['debug'] = False

    available_criteria = get_criteria()

    if kwargs['debug']:
        criteria_for_this_item = set(available_criteria).intersection(set(kwargs.keys()))
        print "\n\tFilename.: %s" % filename
        print "\tDate.....: %s" % name['date']
        print "\tTests....: %s" % criteria_for_this_item
        for ct in criteria_for_this_item:
            print "\t\t%s: %s" % (ct, kwargs[ct])

    for argument_criteria in kwargs.keys():
        if argument_criteria in available_criteria:
            this_criteria = available_criteria[argument_criteria]()
            if kwargs['debug']:
                this_criteria.debugMode = True
            this_criteria.set_argument(kwargs[argument_criteria])
            if not this_criteria.test(filename, name):
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
    if not items:
        return

    if not db and not s3bucket and not ec2snapshots:
        # OS level items
        for item in items:
            this_item = os.path.join(directory, item)
            os.remove(this_item)
    elif not db and s3bucket and not ec2snapshots:
        # S3 items
        s3 = connect_to_s3(aws_access_key_id, aws_secret_access_key)
        bucket = s3.get_bucket(s3bucket)
        for item in items:
            bucket.delete_key(item.key)
    elif not db and not s3bucket and ec2snapshots:
        # EC2 snapshots
        for item in items:
            try:
                item.delete()
            except Exception:
                pass
    else:
        # Database items
        cur = db.cursor()
        for item in items:
            try:
                cur.execute("DROP TABLE %s" % item)
            except Exception:
                pass
