import type { Metadata } from "next";
import { Inter } from "next/font/google"; // Using just Inter for Admin for now
import "./globals.css";
import { Header } from "@/components/header";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "BuyItForLifeClub Admin",
  description: "Admin Dashboard",
};

import { Sidebar } from "@/components/sidebar";
import { auth } from "@/auth";

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const session = await auth();
  const isLoggedIn = !!session?.user;

  return (
    <html lang="en" className="dark">
      <body className={`${inter.className} antialiased bg-background text-foreground min-h-screen`}>
        <Header />
        <div className="flex min-h-[calc(100vh-4rem)]">
          {isLoggedIn && <Sidebar />}
          <main className="flex-1 overflow-x-hidden">{children}</main>
        </div>
      </body>
    </html>
  );
}
