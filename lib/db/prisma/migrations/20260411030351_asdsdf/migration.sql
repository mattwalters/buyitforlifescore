/*
  Warnings:

  - You are about to drop the column `tokenCount` on the `BronzeRedditSubmission` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "BronzeRedditSubmission" DROP COLUMN "tokenCount",
ADD COLUMN     "charCount" INTEGER;
