CREATE TABLE A (
    k INT,
    v INT
);

CREATE TABLE B (
    k INT,
    w INT
);

INSERT INTO A (k, v) VALUES
    (100, 25),
    (100, 25),
    (100, 25),
    (100, 25),

    (200, 20),
    (200, 20),
    (200, 20),
    (500, 50),

    (500, 10),
    (500, 30),
    (500, 10);

-- 4 3 4
-- 100 60 100


INSERT INTO B (k, w) VALUES
    (100, 1),
    (100, 2),
    (200, 3),
    (200, 4),

    (200, 5),
    (500, 6),
    (500, 7);

-- 2 3 2

--count 
-- 8 9 8
-- summ
-- 200 180 200


SELECT A.k, SUM(A.v) AS summ
FROM A
JOIN B ON A.k = B.k
GROUP BY A.k;
