/*
  Warnings:

  - Added the required column `jobName` to the `AiSpend` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "AiSpend" ADD COLUMN     "jobName" TEXT NOT NULL;
