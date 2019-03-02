#! /usr/local/bin/python3
""" Some queries have been coming in truncated. This utility manages a database of query sizes
    and notes when an anomaly is found.
"""
import os
import psycopg2
from psycopg2.extras import NamedTupleCursor

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

cursor.execute("""
  create table if not exists query_sizes (
  id serial primary key,
  query_date timestamp with time zone default current_timestamp,
  query_name text not null,
  query_size integer
  );
  """)

latest_queries = '/Users/vickery/CUNY_Courses/latest_queries/'
fail = False
for file in os.listdir(latest_queries):
  if file.endswith('.csv'):
    file_size = os.stat(f'{latest_queries}/{file}').st_size
    file_name = file.lower()
    cursor.execute(""" select query_date, query_size
                       from query_sizes
                       where query_name = %s
                    """, (file_name, ))
    if cursor.rowcount == 0:
      print(f'Init {file_name}: {file_size:,} bytes')
      cursor.execute('insert into query_sizes values (default, default, %s, %s)',
                     (file_name, file_size))
      if cursor.rowcount != 1:
        print('Insert failed!')
        exit(1)
    else:
      previous_date, previous_size = cursor.fetchone()
      if abs(previous_size - file_size) > (0.1 * previous_size):
        print(f'FAIL: {file_name} ({file_size:,} bytes) is more than 10% different from '
              f'{previous_size:,} bytes'
              f' on {previous_date.strftime("%Y-%m-%d %I:%M %p")}')
        fail = True
      else:
        print(f'Update {file_name}: {file_size:,} bytes')
        cursor.execute('update query_sizes set query_size = %s where query_name = %s',
                       (file_size, file_name))
        if cursor.rowcount != 1:
          print('Update failed!')
          exit(1)
db.commit()
db.close()

if fail:
  exit(1)
exit(0)
