-- Keep track of latest updates to various tables.
-- drop table if exists updates;
-- create table updates (
--   table_name text primary key,
--   update_date text,
--   file_name text default 'N/A');

insert into updates values ('course_mapper', default, default) on conflict do nothing;
insert into updates values ('course_mappings', default, default) on conflict do nothing;
insert into updates values ('cuny_courses', default, 'QNS_QCCV_CU_CATALOG_NP.csv') on conflict do nothing;
insert into updates values ('cuny_institutions', default, 'cuny_institutions.sql') on conflict do nothing;
insert into updates values ('disciplines', default, 'QNS_CV_CUNY_SUBJECT_TABLE.csv') on conflict do nothing;
insert into updates values ('hegis_codes', default, default) on conflict do nothing;
insert into updates values ('nys_institutions', default, default) on conflict do nothing;
insert into updates values ('registered_programs', default, default) on conflict do nothing;
insert into updates values ('requirement_blocks', default, 'dgw_dap_req_block.csv') on conflict do nothing;
insert into updates values ('rule_descriptions', default, default) on conflict do nothing;
insert into updates values ('subjects', default, 'QNS_CV_CUNY_SUBJECTS.csv') on conflict do nothing;
insert into updates values ('transfer_rules', default, 'QNS_CV_SR_TRNS_INTERNAL_RULES.csv') on conflict do nothing;