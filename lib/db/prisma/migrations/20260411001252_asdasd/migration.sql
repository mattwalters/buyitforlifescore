-- AlterTable
ALTER TABLE "GoldProduct" ADD COLUMN     "goldDepartmentId" TEXT;

-- AlterTable
ALTER TABLE "GoldProductLine" ADD COLUMN     "goldDepartmentId" TEXT;

-- CreateTable
CREATE TABLE "GoldDepartment" (
    "id" TEXT NOT NULL,
    "canonicalName" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "GoldDepartment_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GoldCategory" (
    "id" TEXT NOT NULL,
    "canonicalName" TEXT NOT NULL,
    "embedding" vector(1024),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "GoldCategory_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "SilverCategoryIdea" (
    "id" TEXT NOT NULL,
    "goldProductLineId" TEXT,
    "goldProductId" TEXT,
    "rawName" TEXT NOT NULL,
    "isProcessed" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "SilverCategoryIdea_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "_GoldCategoryToGoldProductLine" (
    "A" TEXT NOT NULL,
    "B" TEXT NOT NULL,

    CONSTRAINT "_GoldCategoryToGoldProductLine_AB_pkey" PRIMARY KEY ("A","B")
);

-- CreateTable
CREATE TABLE "_GoldCategoryToGoldProduct" (
    "A" TEXT NOT NULL,
    "B" TEXT NOT NULL,

    CONSTRAINT "_GoldCategoryToGoldProduct_AB_pkey" PRIMARY KEY ("A","B")
);

-- CreateIndex
CREATE UNIQUE INDEX "GoldDepartment_canonicalName_key" ON "GoldDepartment"("canonicalName");

-- CreateIndex
CREATE UNIQUE INDEX "GoldCategory_canonicalName_key" ON "GoldCategory"("canonicalName");

-- CreateIndex
CREATE INDEX "_GoldCategoryToGoldProductLine_B_index" ON "_GoldCategoryToGoldProductLine"("B");

-- CreateIndex
CREATE INDEX "_GoldCategoryToGoldProduct_B_index" ON "_GoldCategoryToGoldProduct"("B");

-- AddForeignKey
ALTER TABLE "SilverCategoryIdea" ADD CONSTRAINT "SilverCategoryIdea_goldProductLineId_fkey" FOREIGN KEY ("goldProductLineId") REFERENCES "GoldProductLine"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "SilverCategoryIdea" ADD CONSTRAINT "SilverCategoryIdea_goldProductId_fkey" FOREIGN KEY ("goldProductId") REFERENCES "GoldProduct"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GoldProductLine" ADD CONSTRAINT "GoldProductLine_goldDepartmentId_fkey" FOREIGN KEY ("goldDepartmentId") REFERENCES "GoldDepartment"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GoldProduct" ADD CONSTRAINT "GoldProduct_goldDepartmentId_fkey" FOREIGN KEY ("goldDepartmentId") REFERENCES "GoldDepartment"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "_GoldCategoryToGoldProductLine" ADD CONSTRAINT "_GoldCategoryToGoldProductLine_A_fkey" FOREIGN KEY ("A") REFERENCES "GoldCategory"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "_GoldCategoryToGoldProductLine" ADD CONSTRAINT "_GoldCategoryToGoldProductLine_B_fkey" FOREIGN KEY ("B") REFERENCES "GoldProductLine"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "_GoldCategoryToGoldProduct" ADD CONSTRAINT "_GoldCategoryToGoldProduct_A_fkey" FOREIGN KEY ("A") REFERENCES "GoldCategory"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "_GoldCategoryToGoldProduct" ADD CONSTRAINT "_GoldCategoryToGoldProduct_B_fkey" FOREIGN KEY ("B") REFERENCES "GoldProduct"("id") ON DELETE CASCADE ON UPDATE CASCADE;
