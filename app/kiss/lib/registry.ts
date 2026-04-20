export interface QualityRule {
  name: string;
  severity: "warn" | "error";
  /**
   * Raw SQL executed against the duckdb target. 
   * Must return exactly one row with a boolean column `passed`.
   * Use '{{target}}' for the R2 target to be swapped at runtime.
   */
  sqlTemplate: string; 
}

export interface PipelineAsset {
  id: string;
  layer: "bronze" | "silver" | "gold";
  isPartitioned: boolean;
  dependencies: string[];
  /**
   * The R2 path expression. 
   * Defaults to 's3://${process.env.R2_BUCKET_NAME}/${layer}/${id}{{suffix}}'.
   * Suffix is conditionally '.parquet' or '/partitionKey.parquet'.
   */
  storagePathTemplate?: string;
  qualityRules: QualityRule[];
}

// -----------------------------------------------------------------------------
// Core Pipeline Pipeline "Code-as-Configuration" Registry
// -----------------------------------------------------------------------------

export const AssetRegistry: Record<string, PipelineAsset> = {
  
  reddit_buyitforlife_submissions: {
    id: "reddit_buyitforlife_submissions",
    layer: "bronze",
    isPartitioned: true,
    dependencies: [], // Root asset
    qualityRules: [
      {
        name: "Submissions Must Have URLs",
        severity: "error",
        sqlTemplate: `
          SELECT COUNT(*) = 0 AS passed 
          FROM read_parquet('{{target}}') 
          WHERE url IS NULL
        `
      }
    ]
  },
  
  reddit_buyitforlife_comments: {
    id: "reddit_buyitforlife_comments",
    layer: "bronze",
    isPartitioned: true,
    dependencies: ["reddit_buyitforlife_submissions"],
    qualityRules: [
      {
        name: "No Missing Author IDs",
        severity: "error",
        sqlTemplate: `
          SELECT COUNT(*) = 0 AS passed 
          FROM read_parquet('{{target}}') 
          WHERE author IS NULL
        `
      },
      {
        name: "Upvotes Cannot Be Anomalously Negative",
        severity: "warn",
        sqlTemplate: `
          SELECT COUNT(*) = 0 AS passed 
          FROM read_parquet('{{target}}') 
          WHERE score < -100
        `
      }
    ]
  },

  // Example Silver Asset for Lineage
  reddit_node_summarizations: {
    id: "reddit_node_summarizations",
    layer: "silver",
    isPartitioned: true,
    dependencies: ["reddit_buyitforlife_comments", "reddit_buyitforlife_submissions"],
    qualityRules: [
      {
        name: "AI Summaries Not Empty",
        severity: "error",
        sqlTemplate: `
          SELECT COUNT(*) = 0 AS passed 
          FROM read_parquet('{{target}}') 
          WHERE summary IS NULL OR length(summary) < 5
        `
      }
    ]
  }

};
