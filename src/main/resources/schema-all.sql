DROP TABLE people IF EXISTS;

CREATE TABLE account  (
    account_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20)
);
