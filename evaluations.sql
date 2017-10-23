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
source_course_id integer,
destination_course_id integer,
rule_priority integer,
rule_group integer,
event_type text,
who text,
what text,
event_time timestamptz default now(),
foreign key (event_type) references event_types(abbr),
foreign key (source_course_id, rule_priority, rule_group, destination_course_id)
  references transfer_rules(source_course_id, rule_priority, rule_group, destination_course_id)
);

-- Populate event_types
insert into event_types (abbr, bitmask, description) values ('src-ok', 1, 'Sender Approve');
insert into event_types (abbr, bitmask, description) values ('dest-ok', 2, 'Receiver Approve');
insert into event_types (abbr, bitmask, description) values ('src-not-ok', 4, 'Sender Problem');
insert into event_types (abbr, bitmask, description) values ('dest-not-ok', 8, 'Receiver Problem');
insert into event_types (abbr, bitmask, description) values ('other', 16, 'Other');
insert into event_types (abbr, bitmask, description) values ('resolve-keep', 32, 'Keep Rule');
insert into event_types (abbr, bitmask, description) values ('resolve-drop', 64, 'Delete Rule');
