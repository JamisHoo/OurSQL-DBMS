USE test;

INSERT INTO table_1 VALUES
(1000, 'VARCHAR000', true, 100),
(1001, 'VARCHAR001', false, 101),
(1002, 'VARCHAR002', NULL, NULL),
(1003, 'VARCHAR003', NULL, 103),
(1004, 'VARCHAR004', true, NULL);

# fail, against check constraint
INSERT INTO table_1 VALUES (0, 'VARCHAR000', true, 100);
INSERT INTO table_1 VALUES (2000, 'VARCHAR000', true, 100);
INSERT INTO table_1 VALUES (1900, 'VARCHARxxx', true, 100);

# fail, literal out of range
INSERT INTO table_1 VALUES (1900, 'VARCHAR900', 1, 2147483648);

# fail, duplicate primary key
INSERT INTO table_1 VALUES (1000, 'VARCHAR000', NULL, 100);


INSERT INTO table_2 VALUES
(-100, 'VARCHAR003', -100.069),
(+101, 'VARCHAR000', +121.999),
(102, 'VARCHAR003', 8782.2121),
(-103, 'VARCHAR003', 0.001),
(103,  NULL, 3892);

# fail, against reference constraint
INSERT INTO table_2 VALUES (190, 'WTF', 999990);

# fail, assert not null
INSERT INTO table_2 VALUES (190, 'VARCHAR001', NULL);

INSERT INTO table_3 VALUES
(-801, 'VARCHAR002', -100),
(82, 'VARCHAR004', 102);

# fail, against check constraint
INSERT INTO table_3 VALUES (83, 'VARCHAR103', NULL);

# fail, should roll back
INSERT INTO table_3 VALUES
(83, 'VARCHAR003', 102),
(84, 'VARCHAR04', 102);

