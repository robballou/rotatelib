Module for assisting in querying the file system and databases for backups to rotate

Sample Python script using rotatelib:

    import datettime
    import rotatelib

    backups = '/my/backups/'

    # find any backups that are older than 5 days
    items = rotatelib.list_archives(directory=backups, before=datetime.timedelta(5))

    # remove those backups we just found
    rotatelib.remove_items(directory=backups, items=items)

You may also now give it database connections to work with:


    import datettime
    import rotatelib
    import MySQLdb

    db = MySQLdb.connect('localhost', 'user', 'password', 'my_database')

    # find any backup tables (tables with a date in the name) that are older than 5 days
    items = rotatelib.list_backup_tables(db=db, before=datetime.timedelta(5))

    # remove those backups we just found
    rotatelib.remove_items(db=db, items=items)