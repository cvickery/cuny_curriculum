-- Roles of users, by institution
-- Each person listed will be notified of events involving their institution.
drop table if exists roles cascade;
drop table if exists person_roles cascade;

create table roles (
  id text primary key,
  description text
);

create table person_roles (
  id serial primary key,
  institution text references institutions default null,
  job_title text,
  role text references roles,
  email text not null,
  name text not null
);

-- Eventually, initialization/editing should be done via web forms. For now, it's ad hoc.

insert into roles values('cuny_registrar', 'University Registrar');
insert into roles values('college_registrar', 'College  Registrar');
insert into roles values('college_provost', 'College Provost');
insert into roles values('webmaster', 'Webmaster');

insert into person_roles values(default, null,
                                'Executive University Registrar',
                                'cuny_registrar',
                                'nobody@cuny.edu', 'University Registrar');
insert into person_roles values(default, null,
                                'Professor of Computer Science',
                                'webmaster',
                                'Christopher.Vickery@qc.cuny.edu', 'Christopher Vickery');
insert into person_roles values(default, 'QNS01',
                                'Associate Provost',
                                'college_provost',
                                'nobody@qc.cuny.edu', 'Alicia Alvero');
