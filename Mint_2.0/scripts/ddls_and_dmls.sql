drop table if exists mint.f_transactions_raw;
drop table if exists mint.f_transactions;
drop table if exists mint.tmp_transactions_raw;
drop table if exists mint.job_logging;


CREATE TABLE IF NOT EXISTS mint.f_transactions_raw (
    dwh_insert_date TIMESTAMP NOT NULL,
    date VARCHAR(1000) NOT NULL,
    description VARCHAR(512),
    amount VARCHAR(1000)
);
        
CREATE TABLE IF NOT EXISTS mint.f_transactions (
    dwh_insert_date VARCHAR(100) NOT NULL,
    date VARCHAR(1000) NOT NULL,
    description VARCHAR(512),
    category VARCHAR(100),
    amount VARCHAR(1000)
);
        
CREATE TABLE IF NOT EXISTS mint.job_logging (
    job_dwh_insert_date TIMESTAMP NOT NULL
);

INSERT INTO mint.f_transactions (dwh_insert_date, date, description, category, amount)
VALUES ('1970-01-01 00:00:00.000000', '1970-01-01', NULL,NULL, NULL);