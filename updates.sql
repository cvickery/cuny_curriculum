-- Keep track of latest updates to various tables.
drop table if exists updates;
create table updates (
  table_name text primary key,
  update_date text,
  file_name text default 'N/A');

insert into updates values ('cuny_courses');
insert into updates values ('cuny_disciplines');
insert into updates values ('cuny_subjects');
insert into updates values ('hegis_codes');
insert into updates values ('nys_institutions');
insert into updates values ('registered_programs');
insert into updates values ('requirement_blocks');
insert into updates values ('transfer_rules');
