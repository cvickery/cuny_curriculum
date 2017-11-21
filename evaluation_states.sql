
-- Create a table of bitmap values for the transfer_rules status column to reference.
drop table if exists transfer_rule_status cascade;
  create table transfer_rule_status (
    value integer primary key,
    description text);

-- Enumerate descriptions for all bits
insert into transfer_rule_status values (0, 'Not Evaluated');
insert into transfer_rule_status values (1, 'Sender Approved');
insert into transfer_rule_status values (2, 'Receiver Approved');
insert into transfer_rule_status values (4, 'Sender Reported Problem');
insert into transfer_rule_status values (8, 'Receiver Reported Problem');
insert into transfer_rule_status values (16, 'Other Issue');
insert into transfer_rule_status values (32, 'Resolved: OK');
insert into transfer_rule_status values (64, 'Resolved: Do Not Use');