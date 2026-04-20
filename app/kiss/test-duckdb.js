const duckdb = require("duckdb");
console.log("Got duckdb:", !!duckdb);
const db = new duckdb.Database(":memory:");
console.log("Memory DB Instantiated");
db.serialize(() => {
    console.log("Serialize started.");
    db.run("INSTALL httpfs;", (err) => {
        console.log("INSTALL callback", err);
    });
    console.log("Finished serialization queue init");
});
console.log("Script end");
