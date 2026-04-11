-- AlterTable
ALTER TABLE "GoldBrand" ADD COLUMN     "isTitled" BOOLEAN NOT NULL DEFAULT false;

-- AlterTable
ALTER TABLE "GoldProduct" ADD COLUMN     "isHierarchyAnalyzed" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "isTitled" BOOLEAN NOT NULL DEFAULT false;

-- AlterTable
ALTER TABLE "GoldProductLine" ADD COLUMN     "isTitled" BOOLEAN NOT NULL DEFAULT false;
