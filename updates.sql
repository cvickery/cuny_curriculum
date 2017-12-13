-- Keep track of latest updates to the catalog and rules tables.
drop table if exists updates;
create table updates (
  table_name text primary key,
  update_date text default 'unknown',
  file_name text default 'unknown');

insert into updates values ('courses');
insert into updates values ('rules');
insert into updates values ('subjects');
insert into updates values ('disciplines');
