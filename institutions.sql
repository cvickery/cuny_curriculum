drop table institutions;
create table institutions (
  code text primary key,
  prompt text,
  name text,
  date_updated date);
--                                code     prompt            name                             date
insert into institutions values ('BAR01', 'Baruch',         'Baruch College',                 '');
insert into institutions values ('BCC01', 'Bronx',          'Bronx Community College',        '');
insert into institutions values ('BKL01', 'Brooklyn',       'Brooklyn College',               '');
insert into institutions values ('BMC01', 'BMCC',           'Borough of Manhattan CC',        '');
insert into institutions values ('CSI01', 'Staten Island',  'College of Staten Island',       '');
insert into institutions values ('CTY01', 'City',           'City College',                   '');
insert into institutions values ('GRD01', 'Grad Center',    'The Graduate Center',            '');
insert into institutions values ('HOS01', 'Hostos',         'Hostos Community College',       '');
insert into institutions values ('HTR01', 'Hunter',         'Hunter College',                 '');
insert into institutions values ('JJC01', 'John Jay',       'John Jay College',               '');
insert into institutions values ('KCC01', 'Kingsborough',   'Kingsborough CC',                '');
insert into institutions values ('LAG01', 'LaGuardia',      'LaGuardia Community College',    '');
insert into institutions values ('LAW01', 'Law School',     'CUNY School of Law',             '');
insert into institutions values ('LEH01', 'Lehman',         'Lehman College',                 '');
insert into institutions values ('MEC01', 'Medgar Evers',   'Medgar Evers College',           '');
insert into institutions values ('MED01', 'Medical School', 'CUNY School of Medicine',        '');
insert into institutions values ('NCC01', 'Guttman',        'Guttman Community College',      '');
insert into institutions values ('NYT01', 'City Tech',      'NYC College of Technology',      '');
insert into institutions values ('QCC01', 'Queensborough',  'Queensborough CC',               '');
insert into institutions values ('QNS01', 'Queens',         'Queens College',                 '');
insert into institutions values ('SPH01', 'Public Health',  'CUNY School of Public Health',   '');
insert into institutions values ('SPS01', 'SPS',            'School of Professional Studies', '');
insert into institutions values ('YRK01', 'York',           'York College', '');