CREATE DATABASE test;

USE test;

CREATE TABLE table_1 (field_1 BIGINT UNSIGNED NOT NULL, 
                      field_2 VARCHAR(30),
                      field_3 BOOL,
                      field_4 INT SIGNED, 
                      PRIMARY KEY(field_2),
                      CHECK (field_1 >= 1000 and field_1 < 2000 and
                             field_2 like 'VARCHAR[[:digit:]]{3}')
                     );

CREATE INDEX ON table_1(field_1);
CREATE INDEX ON table_1(field_4);

DESC table_1;

CREATE TABLE table_2 (field_1 INT SIGNED,
                      field_2 VARCHAR(30),
                      field_3 FLOAT NOT NULL,
                      PRIMARY KEY(field_1),
                      FOREIGN KEY (field_2) REFERENCES table_1(field_2)
                     );

CREATE INDEX ON table_2(field_2);
CREATE INDEX ON table_2(field_3);

DESCRIBE table_2;

CREATE TABLE table_3 (field_1 INT SIGNED,
                      field_2 VARCHAR(30),
                      field_3 INT, # is signed by default
                      CHECK (field_1 <= field_3 and field_3 IS NOT NULL),
                      FOREIGN KEY (field_2) REFERENCES table_1(field_2),
                      FOREIGN KEY (field_3) REFERENCES table_2(field_1)
                     );

CREATE INDEX ON table_3(field_3);
DROP INDEX ON table_3(field_3);
DESC table_3;

SHOW TABLES;


