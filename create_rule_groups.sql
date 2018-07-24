-- The tranfer_rules table has to be replaced by the rule_groups table.
drop table if exists rule_groups cascade;
create table rule_groups (
  source_institution text not null,
  discipline text not null,
  group_number integer not null,
  destination_institution text not null,
  status integer default 0,
  foreign key (source_institution, discipline) references disciplines,
  foreign key (destination_institution) references institutions,
  primary key (source_institution, discipline, group_number, destination_institution));

drop table if exists source_courses cascade;
create table source_courses (
  id serial primary key,
  source_institution text,
  discipline text,
  group_number integer,
  destination_institution text,
  course_id integer, -- may reference multiple rows in courses if cross-listed
  min_gpa real,
  max_gpa real,
  foreign key (source_institution, discipline, group_number, destination_institution)
    references rule_groups,
  unique (source_institution,
          discipline,
          group_number,
          destination_institution,
          course_id,
          min_gpa,
          max_gpa));

drop table if exists destination_courses cascade;
create table destination_courses (
  id serial primary key,
  source_institution text,
  discipline text,
  group_number integer,
  destination_institution text,
  course_id integer, -- may reference multiple rows in courses if cross-listed
  transfer_credits real,
  foreign key (source_institution, discipline, group_number, destination_institution)
    references rule_groups,
 unique (source_institution,
         discipline,
         group_number,
         destination_institution,
         course_id,
         transfer_credits));