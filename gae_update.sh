#! /usr/local/bin/bash

# Reinitialize the GAE copy of the cuny_courses db while maintaining existing review activity.
#
# 2017-12-21: Automating the cleanup
#
# 2017-11-21: To copy the cuny_courses db from here to the cloud is complicated by the fact that
# Google App Engine (GAE) is running an older version of Postgres than I am running on babbage and
# cvlaptop. (Google: 9.6.1; Me: 10.1.)

(
# Need function definition for pgproxy
. ~/.aliases_du_jour > /dev/null 2>&1

gae_restore_file=gae_restore.`date +%Y-%m-%d_%H:%M`.sql
gae_events_file=gae_events.`date +%Y-%m-%d_%H:%M`.sql

# First, be sure sql proxy is not running and create a dump of the local db:
echo "Be sure pgproxy is stopped (Expect 'No matching processes')..."
pgproxy stop
echo "Dumping local db..."
pg_dump -O cuny_courses > $gae_restore_file

# No user associated with tables. On babbage and cvlaptop, vickery is the owner, but on GAE, the
# owner will be postgres.

# There are issues with gae_restore_file that have to be fixed.
#   - The plpgsql extension is not owned by postgres on GAE, so commands related to that generate
#     errors. But they don't prevent a successful restore. This may change if triggers written in
#     plpgsql get written.
#   - In Postgres 10.1, the create sequence command has a new "AS <datatype>" clause that causes an
#     error when restoring to the 9.6.1 server on GAE. You have to delete those lines from
#     gae_restore_file.
echo "Fixing $gae_restore_file ..."
ack -v "plpgsql" $gae_restore_file | ack -v "AS integer" > temp.sql
diff $gae_restore_file temp.sql

read -p "Restore to GAE [Yn]? " reply
if [[ $reply =~ [Nn] ]]
then rm -f temp.sql
      echo "Remove '$gae_restore_file' if you don’t want it."
      echo "Nothing else changed."
      exit
fi
mv temp.sql $gae_restore_file

# Aside: There is an option to specify a directory for the dump, which might be more efficient. But
# the files generated are compressed, so it seems like less bother to use the default format, which
# produces more easily-edited output. The restore took only 17 seconds, so there's not much to be
# gained by generating the more efficient dump directory.

# To restore to the GAE:
#   Start the SQL proxy server, save the events table, delete and recreate the db,
#   restore as user postgres, restore the events table, update rule statuses.
echo Begin rebuilding GAE database

echo "Restore $gae_restore_file to GAE..."
echo "  Starting pgproxy ..."
pgproxy start
sleep 3

echo "  Saving the GAE events table to $gae_events_file ..."
pg_dump --data-only --table=events -f $gae_events_file cuny_courses
sleep 2

echo '  Dropping GAE database...'
psql -U postgres -c 'drop database cuny_courses'
sleep 2

echo '  Creating GAE database...'
psql -U postgres -c 'create database cuny_courses'
sleep 2

echo '  Rebuilding GAE database...'
psql -U postgres cuny_courses < $gae_restore_file > restore.log 2>&1
if [ $? -eq 0 ]
then echo '  ... OK'
else echo "Error: see restore.log for details."
     exit 1
fi

echo "  Restore the events table from $gae_events_file ..."
psql cuny_courses -c "truncate table events restart identity" >> restore.log 2>&1
if [ $? -eq 0 ]
then echo '  ... truncate old table OK'
else echo "Error: see restore.log for details."
     exit 1
fi
sleep 2


psql cuny_courses < $gae_events_file  >> restore.log 2>&1
if [ $? -eq 0 ]
then echo '  ... restore events OK'
else echo "Error: see restore.log for details."
     exit 1
fi
sleep 2

python3 update_review_statuses.py >> restore.log 2>&1
if [ $? -eq 0 ]
then echo '  ... update statuses OK'
else echo "Error: see restore.log for details."
     exit 1
fi
echo Finished rebuilding GAE database.
)
