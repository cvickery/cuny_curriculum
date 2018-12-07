-- Set up role query_only with select and login access

drop role if exists query_only;
create user query_only with login password 'fernández';
grant select on all tables in schema public to query_only;
