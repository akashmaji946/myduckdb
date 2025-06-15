CREATE TABLE A (
    k INT,
    v INT,
    p CHAR,
    q FLOAT
);

CREATE TABLE B (
    a INT,
    k INT,
    w INT,
    x CHAR,
    y FLOAT
);

INSERT INTO A (k, v, p, q) VALUES
    (100, 25, 'A', 1.5),
    (100, 25, 'A', 1.5),
    (100, 25, 'A', 1.5),
    (100, 25, 'A', 1.5),

    (200, 20, 'A', 1.5),
    (200, 20, 'A', 1.5),
    (200, 20, 'A', 1.5),
    (500, 50, 'A', 1.5),

    (500, 10, 'A', 1.5),
    (500, 30, 'A', 1.5),
    (500, 10, 'A', 1.5);

-- 4 3 4
-- 100 60 100


INSERT INTO B (a, k, w, x, y) VALUES
    (1, 100, 1, 'A', 1.5),
    (1, 100, 2, 'A', 1.5),
    (1, 200, 3, 'A', 1.5),
    (1, 200, 4, 'A', 1.5),

    (1, 200, 5, 'A', 1.5),
    (1, 500, 6, 'A', 1.5),
    (1, 500, 7, 'A', 1.5);

-- 2 3 2

--count 
-- 8 9 8
-- summ
-- 200 180 200


SELECT A.k, SUM(A.v) AS summ
FROM A
JOIN B ON A.k = B.k
GROUP BY A.k;
