-- The credit_sources table
drop table if exists credit_sources cascade;
create table credit_sources (
  value text primary key,
  short_name text,
  long_name text
  );
insert into credit_sources values ('C', 'Catalog', 'Use Catalog Units');
insert into credit_sources values ('E', 'External', 'Specify Maximum Units');
insert into credit_sources values ('R', 'Rule', 'Specify Fixed Units');

-- The tranfer_rules table
drop table if exists transfer_rules cascade;
create table transfer_rules (
  id serial primary key,
  rule_key text,
  source_institution text not null,
  destination_institution text not null,
  subject_area text not null,
  group_number integer not null,
  priority integer not null,
  source_disciplines text not null, -- colon-separated list of all source disciplines
  source_subjects text not null, -- colon-separated list of all source course cuny_subjects
  destination_disciplines text not null, -- colon-separated list of all destination disciplines
  destination_subjects text not null, -- colon-separated list of all destination course cuny_subjs
  sending_courses text not null, -- colon-separated list of sending course_id.offer_nbr
  receiving_courses text not null, -- colon-separated list of receiving course_id.offer_nbr
  credit_sources text not null, -- colon-separated src:dst CER values
  review_status integer default 0,
  effective_date date, -- latest effective date of any table/view in CF query
  foreign key (source_institution) references cuny_institutions,
  foreign key (destination_institution) references cuny_institutions);

-- source_courses
drop table if exists source_courses cascade;
create table source_courses (
  id serial primary key,
  rule_id integer references transfer_rules,
  course_id integer,
  offer_nbr integer,
  offer_count integer,  -- greater than 1 for cross-listed courses
  discipline text,
  catalog_number text, -- "the" catalog number
  cat_num real,        -- numeric part for display ordering
  cuny_subject text,
  min_credits real,
  max_credits real,
  credit_source text references credit_sources,
  min_gpa real,
  max_gpa real,
  aliases jsonb);

-- destination_courses
drop table if exists destination_courses cascade;
create table destination_courses (
  id serial primary key,
  rule_id integer references transfer_rules,
  course_id integer,
  offer_nbr integer,
  offer_count integer,  -- greater than 1 for cross-listed courses
  discipline text,
  catalog_number text,  -- "the" catalog number
  cat_num real,         -- numeric part for display ordering
  cuny_subject text,
  transfer_credits real,
  credit_source text references credit_sources,
  course_status text,
  is_mesg boolean,
  is_bkcr boolean);

