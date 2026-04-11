 
 
 
import { prisma } from "@mono/db";
import bcrypt from "bcryptjs";
import readline from "readline";
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const question = (query: string): Promise<string> =>
  new Promise((resolve) => rl.question(query, resolve));

async function main() {
  console.log("=== Setup Monorepo Admin ===");
  try {
    const email = await question("Admin Email: ");
    const password = await question("Admin Password: ");

    if (!email || !password) {
      throw new Error("Email and password are required.");
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const admin = await prisma.admin.upsert({
      where: { email },
      update: {
        passwordHash,
      },
      create: {
        email,
        name: "Admin User",
        passwordHash,
      },
    });

    console.log(`\nSuccess! 🛡️`);
    console.log(`Admin user created/updated: ${admin.email}`);
  } catch (error) {
    console.error("Error creating admin:", error);
  } finally {
    rl.close();
    await prisma.$disconnect();
  }
}

main();
