#!/bin/bash
set -e

echo "================================================================"
echo "                BIFL Data Pipeline Orchestrator"
echo "================================================================"
echo ""

LIMIT_ARG=""
RANDOM_ARG=""

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --random)
      RANDOM_ARG="--random"
      echo "Applying random shuffling!"
      shift
      ;;
    *)
      if [[ "$1" =~ ^[0-9]+$ ]]; then
        LIMIT_ARG="--limit $1"
        echo "Applying limit: $1"
      fi
      shift
      ;;
  esac
done

# 1. Silver Generation
echo "🟢 EXTRACTION PHASE: Bootstrapping Silver Extractions"
echo "This uses Gemini Flash to concurrently analyze Reddit Threads and produce"
echo "structured Silver Mentions. Embeddings are decoupled."
# We ONLY apply the LIMIT_ARG here at the top of the funnel.
# All subsequent steps MUST process their complete backlog, otherwise
# items will get stuck in a partially-processed state indefinitely!
npm run silver:generate -w @mono/admin -- --concurrency 10 $LIMIT_ARG $RANDOM_ARG

# 1B. Silver Embedding
echo ""
echo "🟢 EMBEDDING PHASE: Vectorizing Silver Mentions"
echo "This sweeps the database for mentions without vectors and embeds them."
npm run silver:embed -w @mono/admin -- --concurrency 10

# 2. Gold Generation
echo ""
echo "🟢 ROLLUP PHASE: Clustering Centroids (Brands -> Lines -> Models)"
npm run gold:generate:brands -w @mono/admin
npm run gold:generate:lines -w @mono/admin
npm run gold:generate:models -w @mono/admin

# 3. Titling Phase
echo ""
echo "🟢 TITLING PHASE: Executing LLM Canonical Name Normalization"
npm run gold:titling:brands -w @mono/admin -- --concurrency 10
npm run gold:titling:lines -w @mono/admin -- --concurrency 10
npm run gold:titling:models -w @mono/admin -- --concurrency 10

# 4. Hierarchy Linking
echo ""
echo "🟢 HIERARCHY PHASE: Executing Market Taxonomy Linker"
npm run gold:link -w @mono/admin -- --concurrency 10

# 5. Taxonomy Layer
echo ""
echo "🟢 TAXONOMY PHASE: Generating Organic Categories and Routing Matrix"
npm run taxonomy:seed -w @mono/admin
npm run taxonomy:discover -w @mono/admin -- --concurrency 10
npm run taxonomy:consolidate -w @mono/admin
npm run taxonomy:route -w @mono/admin -- --concurrency 10

echo ""
echo "✅ End-to-End Pipeline Complete!"
echo "Check out your shiny new taxonomy by exporting it via the dashboard."
