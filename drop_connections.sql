-- Drop connections to cuny_curriculum db, except this one.
select pg_terminate_backend(pg_stat_activity.pid)
  from pg_stat_activity
 where pg_stat_activity.datname = 'cuny_curriculum'
   and pid <> pg_backend_pid()
;