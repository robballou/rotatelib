"""
Module for assisting in querying the file system for backups to rotate

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
    Determines if the requested filename is an archive or not
    """
    extensions = ['.gz', '.bz2', '.zip', '.tgz']
    if fn[-3:] in extensions or fn[-4:] in extensions:
        return True
    return False

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
        item['date'] = datetime.date(int(result[0]), int(result[1]), int(result[2]))
    return item

def list_archives(directory='./', items=None, **kwargs):
    """
    List all of the archive files in the directory that meet the criteria (see meets_criteria())
    """
    if not items:
        items = os.listdir(directory)
    items = [archive for archive in items if is_archive(archive) and meets_criteria(directory, archive, **kwargs)]
    return items

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
        pattern
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

def remove_items(directory='./', items=None):
    """
    Delete the items in the directory/items list
    """
    if not items: return
    for item in items:
        this_item = os.path.join(directory, item)
        os.remove(this_item)