-- CreateExtension
CREATE EXTENSION IF NOT EXISTS "vector";

-- CreateEnum
CREATE TYPE "JobStatus" AS ENUM ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED');

-- CreateTable
CREATE TABLE "KissJob" (
    "id" TEXT NOT NULL,
    "assetId" TEXT NOT NULL,
    "partitionKey" TEXT,
    "status" "JobStatus" NOT NULL DEFAULT 'QUEUED',
    "requestedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "startedAt" TIMESTAMP(3),
    "completedAt" TIMESTAMP(3),
    "errorTrace" TEXT,

    CONSTRAINT "KissJob_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "KissMaterialization" (
    "id" TEXT NOT NULL,
    "jobId" TEXT NOT NULL,
    "assetId" TEXT NOT NULL,
    "partitionKey" TEXT,
    "summaryPayload" JSONB NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "KissMaterialization_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "KissJob_assetId_partitionKey_idx" ON "KissJob"("assetId", "partitionKey");

-- CreateIndex
CREATE INDEX "KissJob_status_idx" ON "KissJob"("status");

-- CreateIndex
CREATE UNIQUE INDEX "KissMaterialization_jobId_key" ON "KissMaterialization"("jobId");

-- CreateIndex
CREATE INDEX "KissMaterialization_assetId_partitionKey_createdAt_idx" ON "KissMaterialization"("assetId", "partitionKey", "createdAt" DESC);

-- AddForeignKey
ALTER TABLE "KissMaterialization" ADD CONSTRAINT "KissMaterialization_jobId_fkey" FOREIGN KEY ("jobId") REFERENCES "KissJob"("id") ON DELETE CASCADE ON UPDATE CASCADE;
