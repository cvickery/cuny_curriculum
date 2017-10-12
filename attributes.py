# Clear and re-populate the (course) attributes table.

import psycopg2
import csv

db = psycopg2.connect('dbname=cuny_courses')
cur = db.cursor()
cur.execute('drop table if exists attributes')
cur.execute(
    """
    create table attributes (
      attribute_name text,
      attribute_value text,
      description text,
      primary key (attribute_name, attribute_value))
    """)
with open('./queries/SR742A___CRSE_ATTRIBUTE_VALUE.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      q = """insert into attributes values('{}', '{}', '{}')""".format(
          row[cols.index('crse_attr')],
          row[cols.index('crsatr_val')],
          row[cols.index('formal_description')].replace('\'', '’'))
      print(q)
      cur.execute(q)
  db.commit()
  db.close()
