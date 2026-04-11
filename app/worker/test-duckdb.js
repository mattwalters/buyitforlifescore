import duckdb from "duckdb";

const db = new duckdb.Database(":memory:");

db.all(
  `SELECT * FROM '../../data/BuyItForLife_submissions.parquet' LIMIT 5 OFFSET 5000`,
  (err, res) => {
    if (err) {
      console.error("ERROR:", err);
    } else {
      console.log("SUCCESS length:", res.length);
    }
  },
);
