USE test;

# against reference constraint, should roll back
UPDATE table_1 SET field_2 = 'VARCHAR009'
WHERE field_1 like '100[1-3]';

UPDATE table_1 SET field_2 = 'VARCHAR009'
WHERE field_1 like '100[1-3]' and
      field_1 < 1002;

# against reference constraint, should roll back
DELETE from table_2 
WHERE field_1 > 000;

DELETE from table_2
WHERE field_1 like '.?103';
