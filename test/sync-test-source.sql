CREATE TABLE table1 (
	id int NOT NULL PRIMARY KEY,
	b boolean NOT NULL,
	s varchar(1023),
	x binary(3),
	l longblob
);
INSERT INTO table1 (id, b, s, x) VALUES (1, true, 'test value 1', '\0xy'), (2, false, 'test value 2', '\0xy');
