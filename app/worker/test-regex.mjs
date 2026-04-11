import duckdb from "duckdb";
const db = new duckdb.Database(":memory:");
db.all(
  `
  SELECT id, title, created_utc 
  FROM '../../data/BuyItForLife_submissions.parquet' 
  WHERE created_utc IS NULL OR CAST(created_utc AS VARCHAR) ~ '^[0-9]+$'
  LIMIT 10
`,
  (err, res) => {
    if (err) console.error(err);
    else console.table(res);
  },
);
