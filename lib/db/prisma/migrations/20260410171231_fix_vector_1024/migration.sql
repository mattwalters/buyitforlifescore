-- Drop and recreate vectors for exactly 1024 dimensions
ALTER TABLE "SilverProductMention" DROP COLUMN "embedding";
ALTER TABLE "SilverProductMention" ADD COLUMN "embedding" vector(1024);

ALTER TABLE "GoldProduct" DROP COLUMN "embedding";
ALTER TABLE "GoldProduct" ADD COLUMN "embedding" vector(1024);

ALTER TABLE "GoldBrand" DROP COLUMN "embedding";
ALTER TABLE "GoldBrand" ADD COLUMN "embedding" vector(1024);

ALTER TABLE "GoldProductLine" DROP COLUMN "embedding";
ALTER TABLE "GoldProductLine" ADD COLUMN "embedding" vector(1024);