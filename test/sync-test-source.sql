CREATE TABLE table1 (
	id int NOT NULL PRIMARY KEY,
	b boolean NOT NULL,
	s varchar(255),
	x binary(3)
);
INSERT INTO table1 (id, b, s, x) VALUES (1, true, 'test value 1', '\0xy'), (2, false, 'test value 2', '\0xy');
