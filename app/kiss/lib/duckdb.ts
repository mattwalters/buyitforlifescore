import { env } from "../env";

// Dynamically retrieve duckdb bypassing the Next.js/Turbopack static analyzer
// which has a fatal panic when trying to parse node-pre-gyp's package.json.
export async function getDuckDB() {
  const duckdb = eval("require('duckdb')");

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return new Promise<any>((resolve) => {
    // Instantiate DuckDB
    const db = new duckdb.Database(":memory:");

    // We must run sequentially to ensure extensions are loaded before setting variables
    db.serialize(() => {
      db.run("INSTALL httpfs;");
      db.run("LOAD httpfs;");
      // Set R2 credentials if they exist
      if (env.CLOUDFLARE_ACCOUNT_ID && env.R2_ACCESS_KEY_ID && env.R2_SECRET_ACCESS_KEY) {
        db.run(`SET s3_endpoint='${env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com';`);
        db.run("SET s3_region='auto';");
        db.run(`SET s3_access_key_id='${env.R2_ACCESS_KEY_ID}';`);
        db.run(`SET s3_secret_access_key='${env.R2_SECRET_ACCESS_KEY}';`);
        db.run("SET s3_url_style='vhost';");
      } else {
        console.warn("⚠️ Missing Cloudflare R2 Credentials in ENV. S3 integration might fail.");
      }

      resolve(db);
    });
  });
}

/**
 * Execute a query and return rows using a Promise.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function queryDuckDB<T = any>(db: any, query: string): Promise<T[]> {
  return new Promise((resolve, reject) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    db.all(query, (err: any, res: any) => {
      if (err) reject(err);
      else resolve(res as T[]);
    });
  });
}

/**
 * Run a command without returning rows (like COPY, CREATE, INSERT).
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function execDuckDB(db: any, query: string): Promise<void> {
  return new Promise((resolve, reject) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    db.exec(query, (err: any) => {
      if (err) reject(err);
      else resolve();
    });
  });
}
