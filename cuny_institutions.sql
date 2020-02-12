-- This is a customized institutions table that includes boolean info about types of degrees
-- offered. This table has to be maintained manually as cuny_institutions come and go and/or change
-- what degrees they offer.

drop table if exists cuny_institutions cascade;
create table cuny_institutions (
  code text primary key,
  prompt text,
  name text,
  associates boolean,
  bachelors boolean);
--                                code     prompt            name
insert into cuny_institutions values ('BAR01', 'Baruch',         'Baruch College',                   false, true);
insert into cuny_institutions values ('BCC01', 'Bronx',          'Bronx Community College',          true,  false);
insert into cuny_institutions values ('BKL01', 'Brooklyn',       'Brooklyn College',                 false, true);
insert into cuny_institutions values ('BMC01', 'BMCC',           'Borough of Manhattan CC',          true,  false);
insert into cuny_institutions values ('CSI01', 'Staten Island',  'College of Staten Island',         true,  true);
insert into cuny_institutions values ('CTY01', 'City',           'City College',                     false, true);
insert into cuny_institutions values ('GRD01', 'Graduate Center','The Graduate Center',              false, false);
insert into cuny_institutions values ('HOS01', 'Hostos',         'Hostos Community College',         true,  false);
insert into cuny_institutions values ('HTR01', 'Hunter',         'Hunter College',                   false, true);
insert into cuny_institutions values ('JJC01', 'John Jay',       'John Jay College',                 false, true);
insert into cuny_institutions values ('KCC01', 'Kingsborough',   'Kingsborough Community College',   true,  false);
insert into cuny_institutions values ('LAG01', 'LaGuardia',      'LaGuardia Community College',      true,  false);
insert into cuny_institutions values ('LAW01', 'Law School',     'CUNY School of Law',               false, false);
insert into cuny_institutions values ('LEH01', 'Lehman',         'Lehman College',                   false, true);
insert into cuny_institutions values ('MEC01', 'Medgar Evers',   'Medgar Evers College',             true,  true);
insert into cuny_institutions values ('MED01', 'Medical School', 'CUNY School of Medicine',          false, false);
-- insert into cuny_institutions values ('MHC01', 'Macaulay',       'Macaulay Honors College',          false, true);
insert into cuny_institutions values ('NCC01', 'Guttman',        'Guttman Community College',        true,  false);
insert into cuny_institutions values ('NYT01', 'City Tech',      'NYC College of Technology',        true,  true);
insert into cuny_institutions values ('QCC01', 'Queensborough',  'Queensborough Community College',  true,  false);
insert into cuny_institutions values ('QNS01', 'Queens',         'Queens College',                   false, true);
insert into cuny_institutions values ('SLU01', 'Labor/Urban',    'School of Labor & Urban Studies',  false, true);
insert into cuny_institutions values ('SOJ01', 'Journalism',     'Graduate School of Journalism',    false, false);
insert into cuny_institutions values ('SPH01', 'Public Health',  'School of Public Health',          false, false);
insert into cuny_institutions values ('SPS01', 'SPS',            'School of Professional Studies',   false, true);
insert into cuny_institutions values ('YRK01', 'York',           'York College',                     false, true);
