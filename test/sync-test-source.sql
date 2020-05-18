CREATE TABLE table1 (
	id int NOT NULL PRIMARY KEY,
	b boolean NOT NULL,
	nb boolean DEFAULT NULL,
	s varchar(1023),
	x binary(3),
	l longblob
);
INSERT INTO table1 (id, b, nb, s, x)
VALUES (1, true, null, 'test value 1', '\0xy'), (2, false, true, 'test value 2', '\0xy');
