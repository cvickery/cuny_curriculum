
-- Create a table of bitmap values for the transfer_rules status column to reference.
drop table if exists transfer_rule_status cascade;
  create table transfer_rule_status (
    value integer primary key,
    description text);

-- Populate the bitmap values for the transfer_rules status column.
insert into transfer_rule_status values (0, 'Not Evaluated');
insert into transfer_rule_status values (1, 'Sender Approve');
insert into transfer_rule_status values (2, 'Receiver Approve');
insert into transfer_rule_status values (3, 'Sender and Receiver Approve');
insert into transfer_rule_status values (4, 'Sender Problem');
insert into transfer_rule_status values (6, 'Sender Problem and Receiver Approve');
insert into transfer_rule_status values (8, 'Receiver Problem');
insert into transfer_rule_status values (9, 'Sender Approve and Receiver Problem');
insert into transfer_rule_status values (12, 'Sender and Receiver Problem');
insert into transfer_rule_status values (16, 'Other Problem');
insert into transfer_rule_status values (32, 'Resolved: OK');
insert into transfer_rule_status values (48, 'Resolved: Do not use');