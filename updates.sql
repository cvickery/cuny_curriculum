-- Keep track of latest updates to the catalog and rules tables, and registered_programs table.
drop table if exists updates;
create table updates (
  table_name text primary key,
  update_date text default 'unknown',
  file_name text default 'N/A');

insert into updates values ('courses');
insert into updates values ('transfer_rules');
insert into updates values ('subjects');
insert into updates values ('disciplines');
insert into updates values ('registered_programs');