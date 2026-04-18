import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Kiss Pipeline Dashboard",
  description: "Data quality checks and pipeline visualization",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.className} antialiased bg-background text-foreground min-h-screen`}>
        <div className="flex flex-col min-h-screen">
          <header className="flex h-16 shrink-0 items-center gap-2 border-b px-4">
            <h1 className="font-semibold text-lg">Kiss Dashboard</h1>
          </header>
          <main className="flex-1 overflow-x-hidden p-6">{children}</main>
        </div>
      </body>
    </html>
  );
}
