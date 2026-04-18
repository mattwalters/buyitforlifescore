/* eslint-disable @typescript-eslint/no-explicit-any */

import { prisma } from "@mono/db";

const CORE_DEPARTMENTS = [
  "Apparel & Accessories",
  "Footwear",
  "Kitchen & Dining",
  "Tools & Hardware",
  "Home & Furniture",
  "Electronics & Computers",
  "Outdoor & Camping",
  "Sports & Fitness",
  "Automotive & Garage",
  "Travel & Luggage",
  "Health & Personal Care",
  "Office & Stationery",
  "Baby & Kids",
  "Pet Supplies",
  "Lawn & Garden",
  "Musical Instruments",
  "Hobbies & Crafting",
  "Scientific & Medical",
  "Appliances",
];

async function main() {
  console.log(`[Seed] 🌱 Seeding ${CORE_DEPARTMENTS.length} Gold Departments...`);

  let count = 0;
  for (const dept of CORE_DEPARTMENTS) {
    await prisma.goldDepartment.upsert({
      where: { canonicalName: dept },
      update: {},
      create: { canonicalName: dept },
    });
    count++;
  }

  console.log(`[Seed] ✅ Successfully verified ${count} Top-Down Departments.`);
}

main()
  .catch((e) => {
    console.error("Fatal error:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
