
-- Create a table of bitmap values for the transfer_rules status column and the events event_types
-- columns to reference.
drop table if exists review_status_bits cascade;
  create table review_status_bits (
    bitmask integer primary key,
    abbr  text unique,
    description text);

-- Enumerate descriptions for all bits
insert into review_status_bits values (0,  '',              'Not Evaluated');
insert into review_status_bits values (1,  'src-ok',        'Sender Approved');
insert into review_status_bits values (2,  'dest-ok',       'Receiver Approved');
insert into review_status_bits values (4,  'src-not-ok',    'Sender Reported Problem');
insert into review_status_bits values (8,  'dest-not-ok',   'Receiver Reported Problem');
insert into review_status_bits values (16, 'other',         'Other Issue');
insert into review_status_bits values (32, 'resolve-keep',  'Resolved: OK');
insert into review_status_bits values (64, 'resolve-drop',  'Resolved: Do Not Use');
