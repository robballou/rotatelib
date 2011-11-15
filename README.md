# Rotatelib

Version: 0.7

Module for assisting in querying the file system, databases, or Amazon Web Services (AWS) for backups/archives to rotate.

`rotatelib` supports looking for archives and backups in the following places:

- The local filesystem
- A database (currently tested with with MySQLdb and sqlite)
- AWS services:
  - S3 bucket items
  - EC2 snapshots

## Filesystem example

Sample Python script using rotatelib:

    import datetime
    import rotatelib
    
    backups = '/my/backups/'
    
    # find any backups that are older than 5 days
    items = rotatelib.list_archives(directory=backups, before=datetime.timedelta(5))
    
    # you can also find backups in multiple locations
    items = rotatelib.list_archives(directory=['/backups1/', '/backups2/'], before=datetime.timedelta(5), except_day=[1, 15])
    
    # remove those backups we just found
    rotatelib.remove_items(directory=backups, items=items)

## Database example

You may also now give it database connections to work with:

    import datetime
    import rotatelib
    import MySQLdb

    db = MySQLdb.connect('localhost', 'user', 'password', 'my_database')

    # find any backup tables (tables with a date in the name) that are older than 5 days
    items = rotatelib.list_backup_tables(db=db, before=datetime.timedelta(5))

    # remove those backups we just found
    rotatelib.remove_items(db=db, items=items)

## S3 example

If you have the [boto python library][1] installed, you can even access items in an S3 
bucket:

    import datetime
    import rotatelib
    
    """
    When you call list_archives or remove_items with an s3bucket argument, the library
    will look in your environment variables for AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    If you do not want to use environment variables, you can pass those in as keyword args
    (aws_access_key_id and aws_secret_access_key).
    """
    
    # list all archive items
    items = rotatelib.list_archives(s3bucket='mybucket')
    
    # list all archive items that are older than 5 days
    items = rotatelib.list_archives(s3bucket='mybucket', before=datetime.timedelta(5))
    
    # you can also use the directory option with S3
    items = rotatelib.list_archives(s3bucket='mybucket', before=datetime.timedelta(5), directory='mydirectory')
    
    # or multiple directories
    items = rotatelib.list_archives(s3bucket='mybucket', before=datetime.timedelta(5), directory=['dir1', 'dir2'])
    
    rotatelib.remove_items(items=items, s3bucket='mybucket')

## EC2 example

If you have the [boto python library][1] installed, you can even rotate ec2 snapshots:

    import datetime
    import rotatelib
    
    """
    When you call list_archives or remove_items with an ec2snapshots argument, the library
    will look in your environment variables for AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    If you do not want to use environment variables, you can pass those in as keyword args
    (aws_access_key_id and aws_secret_access_key).
    """
    
    # list all archive items
    items = rotatelib.list_archives(ec2snapshots=True)
    
    # list all archive items that are older than 5 days
    items = rotatelib.list_archives(ec2snapshots=True, before=datetime.timedelta(5))
    
    rotatelib.remove_items(items=items, ec2snapshots=True)

By default, `list_archives` will use the snapshots description to find a date. If not date is found,
it will then try to parse the `start_time` portion of the snapshot information. If you'd prefer just
to use the `start_time`, you can use the `snapshot_use_start_time` option.

    # list all archive items that are older than 5 days, using start_time
    items = rotatelib.list_archives(ec2snapshots=True, before=datetime.timedelta(5), snapshot_use_start_time=True)

Note that the EC2 option will only look at snapshots owned by the account for the credentials that are 
used.

## Criteria

To help query for the items you want, there are a number of criteria tests:

  - after (datetime or timedelta)
  - before (datetime or timedelta)
  - day (int or list of ints)
  - except_day (int or list of ints)
  - except_hour (int or list of ints)
  - except_startswith (string or list of strings)
  - has_date (datetime)
  - hour (int or list of ints)
  - startswith (string or list of strings)
  - pattern (regex)

**New in version 0.6:** `startswith` and `except_startswith` were added.  
**New in version 0.2:** `day` and `except_day` were added. `day`, `hour`, `except_day`, and `except_hour` all accept lists as well.

## License

Copyright (c) 2011 Rob Ballou

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

[1]: http://boto.cloudhackers.com/
