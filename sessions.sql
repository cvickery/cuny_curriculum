-- Create the sessions table
drop table if exists sessions;

create table sessions (
session_key text primary key,
session_dict bytea,
expiration_time float);