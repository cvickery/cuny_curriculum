-- The tranfer_rules table.
drop table if exists transfer_rules cascade;
create table transfer_rules (
  id serial primary key,
  source_institution text not null,
  destination_institution text not null,
  subject_area text not null,
  group_number integer not null,
  source_disciplines text not null, -- colon-separated list of all source course disciplines
  source_subjects text not null, -- colon-separated list of all source course cuny_subjects
  review_status integer default 0,
  foreign key (source_institution) references institutions,
  foreign key (destination_institution) references institutions);
-- primary key (source_institution, destination_institution, subject_area, group_number));

drop table if exists source_courses cascade;
create table source_courses (
  id serial primary key,
  rule_id integer references transfer_rules,
  source_institution text,
  destination_institution text,
  subject_area text,
  group_number integer,
  course_id integer, -- may reference multiple rows in courses if cross-listed
  min_credits real,
  max_credits real,
  min_gpa real,
  max_gpa real);
-- foreign key (source_institution, destination_institution, subject_area, group_number)
--   references transfer_rules);

drop table if exists destination_courses cascade;
create table destination_courses (
  id serial primary key,
  rule_id integer references transfer_rules,
  source_institution text,
  destination_institution text,
  subject_area text,
  group_number integer,
  course_id integer, -- may reference multiple rows in courses if cross-listed
  transfer_credits real);
-- foreign key (source_institution, destination_institution, subject_area, group_number)
--   references transfer_rules);
