-- Roles of users, by institution

drop table if exists roles, person_roles, people cascade;

-- CUNY allows at least three forms of the same email address, so let any one of them link to the
-- person's name. The first one given is the link to the roles and person_roles tables.
create table people (
  email text primary key,
  alternate_emails text default '', -- Colon-separated list of alternate CUNY emails (sso, short, etc)
  name text not null
);

create table roles (
  role_name text primary key,
  description text not null
);

create table person_roles (
  id serial primary key,
  -- A person can have different roles at different institutions, depending on their organization.
  -- Or there can be no institution/organization, where it doesn't matter or there is none
  -- (Webmaster, OUR)
  institution text default '--',
  organization text default '--',
  role text references roles,
  email text not null
);

-- Eventually, initialization/editing should be done via web forms. For now, it’s ad hoc.

insert into roles values('cuny_registrar', 'University Registrar');
insert into roles values('webmaster', 'Webmaster');
insert into roles values('registrar', 'College Registrar');
insert into roles values('provost', 'College Provost');
insert into roles values('evaluator', 'Department Evaluator');

insert into people values('OUR@cuny.edu', '', 'University Registrar');
insert into people values('Christopher.Vickery@qc.cuny.edu',
                          'cvickery@qc.cuny.edu:christopher.vickery09@login.cuny.edu',
                          'Christopher Vickery');
insert into people values('Alexander.Vickery@qc.cuny.edu', '', 'Alex Vickery');
insert into people values('Arpita.Paulemon@qc.cuny.edu', '', 'Arpita Paulemon');
insert into people values('Alicia.Alvero@qc.cuny.edu', '', 'Alicia Alvero');
insert into people values('Alexander.Ott@bcc.cuny.edu', '', 'Alexander Ott');
insert into people values('Eva.Fernandez@qc.cuny.edu', 'efernand@qc.cuny.edu', 'Eva Fernández');

-- Notify the Webmaster of all activity for development purposes
insert into person_roles values(default,
                                default,
                                default,
                                'webmaster',
                                'Christopher.Vickery@qc.cuny.edu');
-- An evaluator submits rule evaluations: ok, delete (with explanation), or replace (with plain text
-- of proposed replacement)
insert into person_roles values(default,
                                'QNS01',
                                'CSCI-QC',
                                'evaluator',
                                'Christopher.Vickery@qc.cuny.edu');
insert into person_roles values(default,
                                'QNS01',
                                'LCD-QC',
                                'evaluator',
                                'Eva.Fernandez@qc.cuny.edu');
insert into person_roles values(default,
                                'BCC01',
                                'MATH-BCC',
                                'evaluator',
                                'Alexander.Ott@bcc.cuny.edu');

-- Notify both sending and receiving provosts when rule evaluations are submitted
-- Notify sending provost when receiving provost either approves or rejects changes based on
-- evaluations submitted
insert into person_roles values(default,
                                'QNS01',
                                default,
                                'provost',
                                'Alicia.Alvero@qc.cuny.edu');

-- Notify receiving registrar when receiving provost approves changes (go-ahead to update CUNYfirst)
insert into person_roles values(default,
                                'QNS01',
                                default,
                                'registrar',
                                'Arpita.Paulemon@qc.cuny.edu');

-- Notify cuny_registrar when receiving provost approves a rule change
insert into person_roles values(default,
                                default,
                                default,
                                'cuny_registrar',
                                'OUR@cuny.edu');
