# Clear and re-populate the (requirement) designations table.

import psycopg
import csv

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor() as cursor:
    cursor.execute('drop table if exists designations cascade')
    cursor.execute("""
        create table designations (
        designation text primary key,
        description text)
        """)
    with open('./latest_queries/QCCV_RQMNT_DESIG_TBL.csv') as csvfile:
      csv_reader = csv.reader(csvfile)
      cols = None
      for row in csv_reader:
        if cols == None:
          row[0] = row[0].replace('\ufeff', '')
          cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
        else:
          q = """insert into designations values('{}', '{}')""".format(
              row[cols.index('designation')],
              row[cols.index('formal_description')].replace('l&Q', 'l & Q').replace('eR', 'e R'))
          cursor.execute(q)
      cursor.execute("insert into designations values ('', 'No Designation')")
