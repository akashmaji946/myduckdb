CREATE TABLE stud (
    sid INT,
    sname VARCHAR(50),
    sage INT
);

CREATE TABLE enrol (
    sid INT,
    cid INT
);

INSERT INTO stud (sid, sname, sage) VALUES
    (101, 'A', 1001),
    (102, 'B', 1002);


INSERT INTO enrol (sid, cid) VALUES
    (103, 10001),
    (104, 10002);


SELECT enrol.cid, COUNT(stud.sid) AS student_count
FROM enrol
JOIN stud ON stud.sid != enrol.sid
GROUP BY enrol.cid;
