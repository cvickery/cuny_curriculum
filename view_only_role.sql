-- Set up role view_only with select and login access

-- drop role if exists view_only;
-- create user view_only with login password 'Fernández';
grant connect on database cuny_curriculum to view_only;
grant select on all tables in schema public to public;
alter default privileges in schema public grant select on tables to public;
