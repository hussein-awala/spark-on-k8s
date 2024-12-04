CREATE TABLE students (
    name VARCHAR(64),
    address VARCHAR(64),
    added_at TIMESTAMP
)
USING PARQUET
PARTITIONED BY (student_id INT);

INSERT INTO students VALUES
    ('Amy Smith', '123 Park Ave, San Jose', {{ ts }}, 111111);

INSERT INTO students VALUES
    ('Bob Brown', '456 Taylor St, Cupertino', {{ ts }}, 222222);,
    ('Cathy Johnson', '789 Race Ave, Palo Alto', {{ ts}}, 333333);
