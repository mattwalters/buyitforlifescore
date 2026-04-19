import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { Sidebar } from "../components/sidebar";
import Link from "next/link";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Kiss Asset Dashboard",
  description: "Medallion Architecture Object Explorer",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.className} antialiased bg-background text-foreground min-h-screen`}>
        <div className="flex flex-col min-h-screen relative">
          <header className="flex h-16 shrink-0 items-center justify-between border-b px-6 bg-card sticky top-0 z-10">
            <Link href="/" className="font-semibold text-lg hover:text-primary transition-colors">
              💋 Kiss Pipeline Explorer
            </Link>
            <div className="text-sm text-muted-foreground">Medallion View</div>
          </header>

          <div className="flex flex-1 overflow-hidden">
            <Sidebar />
            <main className="flex-1 overflow-y-auto p-6 bg-background/50">{children}</main>
          </div>
        </div>
      </body>
    </html>
  );
}
