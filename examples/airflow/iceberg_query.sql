CREATE TABLE IF NOT EXISTS prod.db.sample (
    id bigint,
    data string,
    category string
)
USING iceberg
PARTITIONED BY (category);

MERGE INTO prod.db.sample t
USING (SELECT * FROM prod.db.another_sample WHERE category = 'foo') s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
