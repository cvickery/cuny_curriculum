#! /usr/local/bin/bash

# 2017-11-21

# To copy the cuny_courses db from here to the cloud is complicated by the fact that Google App Engine
# (GAE) is running an older version of Postgres than I am running on babbage and cvlaptop.
# (Google: 9.6.1; Me: 10.1)

(
 . ~/.aliases_du_jour
# First, be sure sql proxy is not running and create a dump of the local db:
echo Dumping ...
restore_file=restore.`date +%Y-%m-%d`.sql
pgproxy stop
pg_dump -O cuny_courses > $restore_file

read -p "Edit $restore_file before continuing ..." reply

# No user associated with tables. On babbage and cvlaptop, vickery is the owner, but on GAE, the
# owner will be postgres.

# Now there are issues with restore.sql that have to be fixed.
#   - The plpgsql extension is not owned by postgres on GAE, so commands related to that generate
#   errors. But they don't prevent a successful restore. This may change if triggers written in
#   plpgsql get written.
#   - At the time I am writing this, everything was owned by vickery and I edited the sql to change
#   all to postgres. This should be unnecessary once these instructions are debugged.
#   - In Postgres 10.1, the create sequence command has a new "AS <datatype>" clause that causes an
#   error when restoring to the 9.6.1 server. I just deleted that line from the SQL, and the
#   restore went fine.

# There is an option to specify a directory for the dump, which might be more efficient. But the
# files generated are compressed, so it seems like less bother to use the default format, which
# produces more easily-edited output. The restore took only 17 seconds, so there's not much to be
# gained by generating the more efficient dump directory.

# To restore to the GAE:
#   Start the SQL proxy server, delete and recreate the db, and restore as user postgres:
echo Restoring ...
pgproxy start
sleep 3
echo '  drop database...'
psql -U postgres -c 'drop database cuny_courses'
sleep 2
echo '  create database...'
psql -U postgres -c 'create database cuny_courses'
sleep 2
echo '  resstore database...'
psql -U postgres cuny_courses < $restore_file > restore.log 2>&1

)