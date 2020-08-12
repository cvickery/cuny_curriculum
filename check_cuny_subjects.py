#! /usr/local/bin/python3

import csv

with open('./latest_queries/QNS_CV_CUNY_SUBJECT_TABLE.csv') as csv_file:
  csv_reader = csv.reader(csv_file)
  for row in csv_reader:
    print(row)
    exit()
