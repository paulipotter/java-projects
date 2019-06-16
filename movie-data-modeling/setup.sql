-- Task 1
-- Customer Database
DROP TABLE IF EXISTS plan;
CREATE TABLE plan(
    plan_id int,
    name varchar(20) NOT NULL,
    fee int NOT NULL,
    max_rentals int NOT NULL,
    PRIMARY KEY (plan_id)
);
INSERT INTO plan(plan_id, name, fee, max_rentals)
VALUES
    (1, 'basic', 5, 1),
    (2, 'bronze', 6, 2),
    (3, 'silver', 7, 3),
    (4, 'gold', 8, 4);
--
DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
    cid int,
    first_name varchar(20) NOT NULL,
    last_name varchar(20) NOT NULL,
    plan int NOT NULL REFERENCES plan(plan_id),
    username varchar(30) NOT NULL,
    password varchar(30) NOT NULL,
    unique(username),
    PRIMARY KEY(cid)
);

INSERT INTO customer (cid, first_name, last_name, plan)
VALUES 
(1,'Penny', 'Wood', 1, 'pwood', 1234),
(2, 'Guadalupe', 'Watts',2, 'gwatts', 1234),
(3, 'Kelly', 'Clark',2, 'kellyc', 1234),
(4, 'Milton', 'Benson',3, 'benson', 1234),
(5, 'Terri', 'Dunn', 4, 'terri', 1234);
--
DROP TABLE IF EXISTS rental;
CREATE TABLE rental(
    cid int REFERENCES customer(cid),
    movie_id varchar(20),
    status varchar(10) NOT NULL,
    times_rented int NOT NULL,
	PRIMARY KEY(cid, movie_id)
);
