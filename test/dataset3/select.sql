USE test;

SELECT * FROM table_1;

SELECT COUNT(*) FROM table_2;

SELECT MAX(field_2), AVG(field_1), MIN(field_3), COUNT(field_4), SUM(field_1) FROM table_1;

SELECT *, *, *, field_3 FROM table_3 ORDER BY field_3 DESC;

SELECT field_1, field_2, field_3 FROM table_2 
WHERE field_2 like 'VA.{3}AR00(0|3)' and
      field_1 > 0;

# display fields order is undefined
SELECT table_1.field_1, SUM(table_2.field_3), AVG(table_3.field_1)
FROM table_1, table_2, table_3 
WHERE table_1.field_4 < table_3.field_3
GROUP BY table_1.field_1
ORDER BY table_1.field_1 DESC;
