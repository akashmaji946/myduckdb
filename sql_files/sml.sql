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
    (101, 'A', 25),
    (102, 'B', 26),
    (103, 'A', 27),
    (104, 'B', 23),
    (105, 'A', 30),
    (107, 'D', 30),
    (108, 'C', 25);

INSERT INTO enrol (sid, cid) VALUES
    (101, 1),
    (101, 2),
    (102, 3),
    (103, 4),
    (102, 2),
    (105, 1),
    (108, 1);

-- Query 01
SELECT stud.sid, enrol.cid 
FROM stud JOIN enrol 
ON stud.sid = enrol.sid;

-- Query 02
SELECT stud.sid, enrol.cid 
FROM stud JOIN enrol 
ON stud.sid = enrol.sid 
WHERE stud.sage > 25;

-- Query 03
SELECT enrol.cid, COUNT(stud.sid) AS student_count
FROM stud
JOIN enrol ON stud.sid = enrol.sid
GROUP BY enrol.cid;
