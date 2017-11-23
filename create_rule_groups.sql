-- The tranfer_rules table has to be replaced by the rule_groups table.
drop table if exists rule_groups cascade;
create table rule_groups (
  id serial primary key,
  source_institution text,
  discipline text,
  group_number integer not null,
  destination_institution text,
  status integer default 0,
  foreign key (source_institution, discipline) references disciplines,
  foreign key (destination_institution) references institutions,
  unique (source_institution, discipline, group_number, destination_institution));

drop table if exists source_courses cascade;
create table source_courses (
  id serial primary key,
  rule_group integer references rule_groups,
  course_id integer references courses,
  min_gpa real,
  max_gpa real,
  unique (rule_group, course_id, min_gpa, max_gpa));

drop table if exists destination_courses cascade;
create table destination_courses (
  id serial primary key,
  rule_group integer references rule_groups,
  course_id integer references courses,
  transfer_credits real,
  unique (rule_group, course_id, transfer_credits));