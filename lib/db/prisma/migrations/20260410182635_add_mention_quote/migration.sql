/*
  Warnings:

  - Added the required column `quote` to the `SilverProductMention` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "SilverProductMention" ADD COLUMN     "quote" TEXT NOT NULL;
