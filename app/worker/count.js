import duckdb from "duckdb";
const db = new duckdb.Database(":memory:");
db.all(
  `SELECT count(*) as count FROM '../../data/BuyItForLife_submissions.parquet'`,
  (err, res) => {
    console.log("Submissions count:", res);
  },
);
db.all(`SELECT count(*) as count FROM '../../data/BuyItForLife_comments.parquet'`, (err, res) => {
  console.log("Comments count:", res);
});
