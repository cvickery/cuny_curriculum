-- The pending_evaluations table
drop table if exists pending_evaluations;
create table pending_evaluations (
token text primary key,
email text,
evaluations text,
when_entered timestamptz default now()
);

-- Events and Event-Types schemata.
drop table if exists events;
drop table if exists event_types;

create table event_types (
abbr text primary key,
bitmask integer,
description text,
foreign key (bitmask) references transfer_rule_status);

create table events (
id serial primary key,
rule_group integer references rule_groups,
event_type text,
who text,
what text,
event_time timestamptz default now(),
foreign key (event_type) references event_types(abbr)
);

-- Populate event_types
insert into event_types (abbr, bitmask, description) values
  ('src-ok', 1, (select description from transfer_rule_status where value = 1));
insert into event_types (abbr, bitmask, description) values
  ('dest-ok', 2, (select description from transfer_rule_status where value = 2));
insert into event_types (abbr, bitmask, description) values
  ('src-not-ok', 4, (select description from transfer_rule_status where value = 4));
insert into event_types (abbr, bitmask, description) values
  ('dest-not-ok', 8, (select description from transfer_rule_status where value = 8));
insert into event_types (abbr, bitmask, description) values
  ('other', 16, (select description from transfer_rule_status where value = 16));
insert into event_types (abbr, bitmask, description) values
  ('resolve-keep', 32, (select description from transfer_rule_status where value = 32));
insert into event_types (abbr, bitmask, description) values
  ('resolve-drop', 64, (select description from transfer_rule_status where value = 64));
