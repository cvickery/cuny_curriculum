-- Define the registered programs db
drop table if exists registered_programs;
create table registered_programs (
target_institution text not null,
program_code text not null,
unit_code text not null,
institution text not null,
title text default 'unnamed',
award text not null,
hegis text default 'none',
certificate_license text default 'unknown',
accreditation text default 'unknown',
first_registration_date text default 'unknown',
last_registration_date text default 'unknown',
tap text default 'unknown',
apts text default 'unknown',
vvta text default 'unknown',
primary key (target_institution, institution, program_code, award, hegis)
)