import duckdb from "duckdb";

const db = new duckdb.Database(":memory:");

console.log("Querying BuyItForLife_submissions.parquet for bad created_utc values...");
db.all(
  `
  SELECT id, title, created_utc 
  FROM '../../data/BuyItForLife_submissions.parquet' 
  WHERE created_utc IS NULL OR created_utc = '' OR typeof(created_utc) != 'BIGINT'
  LIMIT 10
`,
  (err, res) => {
    if (err) {
      console.error("Query by type failed, trying regex...", err.message);
      db.all(
        `
      SELECT id, title, created_utc 
      FROM '../../data/BuyItForLife_submissions.parquet' 
      WHERE created_utc IS NULL OR CAST(created_utc AS VARCHAR) !~ '^[0-9]+$'
      LIMIT 10
    `,
        (err2, res2) => {
          if (err2) {
            console.error("Regex query failed:", err2.message);
          } else {
            console.log("Sample of bad created_utc (using regex):");
            console.table(res2);
          }
        },
      );
    } else {
      console.log("Sample of bad created_utc (using type check/null):");
      console.table(res);
    }
  },
);
