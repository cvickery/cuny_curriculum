#! /usr/local/bin/python3
"""Populate the tables of internal (cuny_disciplines) and external (cuny_subjects) subject areas."""

import argparse
import csv
import os
import psycopg
import re
import sys

from collections import namedtuple
from cuny_divisions import ignore_institutions
from datetime import date
from pathlib import Path
from psycopg.rows import namedtuple_row


parser = argparse.ArgumentParser('Create internal and external subject tables')
parser.add_argument('--debug', '-d', action='store_true')
args = parser.parse_args()

with psycopg.connect('dbname=cuny_curriculum') as db:
  with db.cursor(row_factory=namedtuple_row) as cursor:

    # Internal subject (disciplines) and external subject area (cuny_subjects) queries
    discp_file = Path('./latest_queries/QNS_CV_CUNY_SUBJECT_TABLE.csv')
    extern_file = Path('./latest_queries/QNS_CV_CUNY_SUBJECTS.csv')
    for file in [discp_file, extern_file]:
      assert file.is_file(), f'{file.name} does not exist'
    discp_date = date.fromtimestamp(discp_file.stat().st_ctime)
    extern_date = date.fromtimestamp(extern_file.stat().st_ctime)

    cursor.execute("""
                   update updates
                   set update_date = %s, file_name = %s
                   where table_name = 'disciplines'""", (discp_date, discp_file.name))
    cursor.execute("""
                   update updates
                   set update_date = %s, file_name = %s
                   where table_name = 'subjects'""", (extern_date, extern_file.name))

    if args.debug:
      print(f'cuny_subjects.py:\n  cuny_disciplines: {discp_file}\n  cuny_subjects: {extern_file}')

    # Get list of known departments
    cursor.execute("""
                    select department
                    from cuny_departments
                   """)
    departments = [d.department for d in cursor.fetchall()]

    # The cuny_subjects table
    # -------------------------------------------------------------------------------------------------
    cursor.execute('drop table if exists cuny_subjects cascade')
    cursor.execute("""
      create table cuny_subjects (
      subject text primary key,
      subject_name text  )
      """)

    # Populate cuny_subjects
    cursor.execute("insert into cuny_subjects values('missing', 'MISSING')")
    with open(extern_file) as csvfile:
      csv_reader = csv.reader(csvfile)
      cols = None
      for line in csv_reader:
        if cols is None:
          line[0] = line[0].replace('\ufeff', '')
          cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
          Row = namedtuple('Row', cols)
        else:
          row = Row._make(line)
          q = 'insert into cuny_subjects values(%s, %s)'
          cursor.execute(q, (row.external_subject_area, row.description.replace("'", "’")))
      db.commit()

    # The cuny_disciplines table
    # -------------------------------------------------------------------------------------------------
    cursor.execute('drop table if exists cuny_disciplines cascade')
    cursor.execute(
        """
        create table cuny_disciplines (
          institution text references cuny_institutions,
          department text references cuny_departments,
          discipline text,
          discipline_name text,
          cip_code text default NULL,
          hegis_code text default NULL,
          status text,
          cuny_subject text default 'missing' references cuny_subjects,
          primary key (institution, discipline))
        """)

    # Populate cuny_disciplines

    #
    # TEMPORARY: Add missing disciplines for courses that currently don't have one.
    #   These two schools use a non-existent discipline name for certain BKCR/MESG courses.
    #   Two years later: looks like it’s not so temporary.
    #
    Discipline = namedtuple('Discipline',
                            'institution department discipline discipline_name '
                            'cip_code hegis_code status cuny_subject')
    missing_disciplines = [Discipline._make(x) for x in [
        ('SPS01', 'SPS01', 'HESA', 'Temporary Discipline', None, None, 'A', 'ELEC'),
        ('QCC01', 'QCC01', 'ELEC', 'Temporary Discipline', None, None, 'A', 'ELEC')]]
    for discp in missing_disciplines:
      cursor.execute(f"""
                      insert into cuny_disciplines values (
                      '{discp.institution}',
                      '{discp.department}',
                      '{discp.discipline}',
                      '{discp.discipline_name}',
                      '{discp.cip_code}',
                      '{discp.hegis_code}',
                      '{discp.status}',
                      '{discp.cuny_subject}'
                      )
                     """)
    Discipline_Key = namedtuple('Discipline_Key', 'institution discipline')
    discipline_keys = set()
    with open(discp_file) as csvfile:
      csv_reader = csv.reader(csvfile)
      cols = None
      for line in csv_reader:
        if cols is None:
          line[0] = line[0].replace('\ufeff', '')
          if line[0] == 'Institution':
            cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
            Row = namedtuple('Row', cols)
        else:
          row = Row._make(line)
          if row.acad_org in departments:
            if row.institution not in ignore_institutions:
              external_subject_area = row.external_subject_area
              if external_subject_area == '':
                external_subject_area = 'missing'
              discipline_key = Discipline_Key._make([row.institution, row.subject])
              if discipline_key in discipline_keys:
                continue
              discipline_keys.add(discipline_key)
              cursor.execute("""insert into cuny_disciplines values (%s, %s, %s, %s, %s, %s, %s, %s)
                             """, (row.institution,
                                   row.acad_org,
                                   row.subject,
                                   row.formal_description.replace('\'', '’'),
                                   row.cip_code,
                                   row.hegis_code,
                                   row.status,
                                   external_subject_area))
