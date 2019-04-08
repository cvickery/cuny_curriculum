-- Set up role view_only with select and login access

drop role if exists view_only;
create user view_only with login password 'Fernández';
grant select on all tables in schema public to view_only;
