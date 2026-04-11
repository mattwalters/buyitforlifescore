-- CreateExtension
CREATE EXTENSION IF NOT EXISTS "vector";

-- CreateTable
CREATE TABLE "Admin" (
    "id" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "name" TEXT,
    "passwordHash" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Admin_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "BronzeRedditSubmission" (
    "id" TEXT NOT NULL,
    "redditId" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "selftext" TEXT,
    "author" TEXT,
    "score" INTEGER NOT NULL DEFAULT 0,
    "url" TEXT,
    "permalink" TEXT,
    "numComments" INTEGER NOT NULL DEFAULT 0,
    "postedAt" TIMESTAMP(3) NOT NULL,
    "isProcessed" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "BronzeRedditSubmission_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "BronzeRedditComment" (
    "id" TEXT NOT NULL,
    "redditId" TEXT NOT NULL,
    "submissionId" TEXT NOT NULL,
    "linkId" TEXT NOT NULL,
    "parentId" TEXT,
    "body" TEXT NOT NULL,
    "author" TEXT,
    "score" INTEGER NOT NULL DEFAULT 0,
    "postedAt" TIMESTAMP(3) NOT NULL,
    "isProcessed" BOOLEAN NOT NULL DEFAULT false,

    CONSTRAINT "BronzeRedditComment_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GoldBrand" (
    "id" TEXT NOT NULL,
    "canonicalName" TEXT NOT NULL,
    "avgSentiment" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "mentionCount" INTEGER NOT NULL DEFAULT 0,
    "embedding" vector(768),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "GoldBrand_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GoldProductLine" (
    "id" TEXT NOT NULL,
    "goldBrandId" TEXT NOT NULL,
    "canonicalName" TEXT NOT NULL,
    "brand" TEXT NOT NULL,
    "avgSentiment" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "mentionCount" INTEGER NOT NULL DEFAULT 0,
    "embedding" vector(768),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "GoldProductLine_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GoldProduct" (
    "id" TEXT NOT NULL,
    "goldBrandId" TEXT NOT NULL,
    "goldProductLineId" TEXT,
    "canonicalName" TEXT NOT NULL,
    "brand" TEXT NOT NULL,
    "avgSentiment" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "mentionCount" INTEGER NOT NULL DEFAULT 0,
    "embedding" vector(768),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "GoldProduct_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "SilverProductMention" (
    "id" TEXT NOT NULL,
    "submissionId" TEXT,
    "commentId" TEXT,
    "goldBrandId" TEXT,
    "goldProductLineId" TEXT,
    "goldProductId" TEXT,
    "brand" TEXT NOT NULL,
    "productName" TEXT NOT NULL,
    "specificityLevel" TEXT NOT NULL DEFAULT 'UNKNOWN',
    "acquiredPrice" DOUBLE PRECISION,
    "ownershipDurationMonths" INTEGER,
    "usageFrequency" TEXT,
    "durability" TEXT,
    "repairability" TEXT,
    "maintenance" TEXT,
    "warranty" TEXT,
    "value" TEXT,
    "sentiment" TEXT NOT NULL,
    "flawOrCaveat" TEXT,
    "embedding" vector(768),

    CONSTRAINT "SilverProductMention_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "AiSpend" (
    "id" TEXT NOT NULL,
    "submissionId" TEXT,
    "model" TEXT NOT NULL,
    "promptTokens" INTEGER NOT NULL DEFAULT 0,
    "cachedTokens" INTEGER NOT NULL DEFAULT 0,
    "responseTokens" INTEGER NOT NULL DEFAULT 0,
    "totalTokens" INTEGER NOT NULL DEFAULT 0,
    "costInUsd" DOUBLE PRECISION NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "AiSpend_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Admin_email_key" ON "Admin"("email");

-- CreateIndex
CREATE UNIQUE INDEX "BronzeRedditSubmission_redditId_key" ON "BronzeRedditSubmission"("redditId");

-- CreateIndex
CREATE INDEX "BronzeRedditSubmission_score_idx" ON "BronzeRedditSubmission"("score" DESC);

-- CreateIndex
CREATE INDEX "BronzeRedditSubmission_postedAt_idx" ON "BronzeRedditSubmission"("postedAt" DESC);

-- CreateIndex
CREATE UNIQUE INDEX "BronzeRedditComment_redditId_key" ON "BronzeRedditComment"("redditId");

-- CreateIndex
CREATE UNIQUE INDEX "GoldBrand_canonicalName_key" ON "GoldBrand"("canonicalName");

-- AddForeignKey
ALTER TABLE "BronzeRedditComment" ADD CONSTRAINT "BronzeRedditComment_submissionId_fkey" FOREIGN KEY ("submissionId") REFERENCES "BronzeRedditSubmission"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GoldProductLine" ADD CONSTRAINT "GoldProductLine_goldBrandId_fkey" FOREIGN KEY ("goldBrandId") REFERENCES "GoldBrand"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GoldProduct" ADD CONSTRAINT "GoldProduct_goldBrandId_fkey" FOREIGN KEY ("goldBrandId") REFERENCES "GoldBrand"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GoldProduct" ADD CONSTRAINT "GoldProduct_goldProductLineId_fkey" FOREIGN KEY ("goldProductLineId") REFERENCES "GoldProductLine"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "SilverProductMention" ADD CONSTRAINT "SilverProductMention_submissionId_fkey" FOREIGN KEY ("submissionId") REFERENCES "BronzeRedditSubmission"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "SilverProductMention" ADD CONSTRAINT "SilverProductMention_commentId_fkey" FOREIGN KEY ("commentId") REFERENCES "BronzeRedditComment"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "SilverProductMention" ADD CONSTRAINT "SilverProductMention_goldBrandId_fkey" FOREIGN KEY ("goldBrandId") REFERENCES "GoldBrand"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "SilverProductMention" ADD CONSTRAINT "SilverProductMention_goldProductLineId_fkey" FOREIGN KEY ("goldProductLineId") REFERENCES "GoldProductLine"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "SilverProductMention" ADD CONSTRAINT "SilverProductMention_goldProductId_fkey" FOREIGN KEY ("goldProductId") REFERENCES "GoldProduct"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "AiSpend" ADD CONSTRAINT "AiSpend_submissionId_fkey" FOREIGN KEY ("submissionId") REFERENCES "BronzeRedditSubmission"("id") ON DELETE CASCADE ON UPDATE CASCADE;
