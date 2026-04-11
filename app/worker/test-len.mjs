import duckdb from "duckdb";
const db = new duckdb.Database(":memory:");
db.all(
  `
  SELECT id, created_utc, LENGTH(CAST(created_utc AS VARCHAR)) as len 
  FROM '../../data/BuyItForLife_submissions.parquet' 
  LIMIT 5
`,
  (err, res) => {
    if (err) console.error(err);
    else console.table(res);
  },
);
