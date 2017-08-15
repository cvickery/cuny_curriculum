-- The pending_evaluations table
drop table if exists pending_evaluations;
create table pending_evaluations (
token text primary key,
evaluations text,
timestamp datetime default current_timestamp
);

-- Events and Event-Types schemata.
drop table if exists events;
drop table if exists event_types;

create table event_types (
abbr text primary key,
description text);

create table events (
id integer primary key autoincrement,
src_id integer,
dest_id integer,
event_type text,
who text,
what text,
at real default (datetime('now','localtime')),
foreign key (event_type) references event_types(abbr)
foreign key (src_id, dest_id) references transfer_rules(source_course_id, destination_course_id)
);

-- Populate event_types
insert into event_types (abbr, description) values ('send-ok', 'Sender Approve');
insert into event_types (abbr, description) values ('send-err', 'Sender Problem');
insert into event_types (abbr, description) values ('recv-ok', 'Receiver Approve');
insert into event_types (abbr, description) values ('recv-err', 'Receiver Problem');
insert into event_types (abbr, description) values ('other', 'Other');
insert into event_types (abbr, description) values ('resolve-ok', 'Kept');
insert into event_types (abbr, description) values ('resolve-drop', 'Deleted');
