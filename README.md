# Rotatelib

Module for assisting in querying the file system and databases for backups to rotate

## Filesystem example

Sample Python script using rotatelib:

    import datettime
    import rotatelib

    backups = '/my/backups/'

    # find any backups that are older than 5 days
    items = rotatelib.list_archives(directory=backups, before=datetime.timedelta(5))

    # remove those backups we just found
    rotatelib.remove_items(directory=backups, items=items)

## Database example

You may also now give it database connections to work with:

    import datettime
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
    
    rotatelib.remove_items(items=items, s3bucket='mybucket')


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