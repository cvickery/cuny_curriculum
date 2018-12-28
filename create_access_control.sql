-- Access-control table for access_control db

drop table if exists access_control;
create table access_control (
  event_type text unique,
  start_time timestamp
);

insert into access_control values ('update_db', NULL);
insert into access_control values ('maintenance', NULL);