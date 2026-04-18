"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Activity,
  MessageSquare,
  Package,
  Trophy,
  Layers,
  FolderTree,
  Tags,
  Lightbulb,
} from "lucide-react";

export function Sidebar() {
  const pathname = usePathname();

  const links = [
    {
      title: "Dashboard",
      href: "/",
      icon: <LayoutDashboard className="h-5 w-5" />,
    },
    {
      title: "Bronze (Threads)",
      href: "/submissions",
      icon: <MessageSquare className="h-5 w-5" />,
    },
    {
      title: "Silver (Mentions)",
      href: "/silver",
      icon: <Package className="h-5 w-5" />,
    },
    {
      title: "Silver (Ideas)",
      href: "/silver/category-ideas",
      icon: <Lightbulb className="h-5 w-5" />,
    },
    {
      title: "Gold (Depts)",
      href: "/gold/departments",
      icon: <FolderTree className="h-5 w-5" />,
    },
    {
      title: "Gold (Categories)",
      href: "/gold/categories",
      icon: <Tags className="h-5 w-5" />,
    },
    {
      title: "Gold (Brands)",
      href: "/gold/brands",
      icon: <Trophy className="h-5 w-5" />,
    },
    {
      title: "Gold (Lines)",
      href: "/gold/lines",
      icon: <Layers className="h-5 w-5" />,
    },
    {
      title: "Gold (Models)",
      href: "/gold/models",
      icon: <Package className="h-5 w-5" />,
    },
    {
      title: "Background Jobs",
      href: "/jobs",
      icon: <Activity className="h-5 w-5" />,
    },
  ];

  return (
    <aside className="w-64 border-r bg-muted/40 p-4 h-[calc(100vh-4rem)] sticky top-16">
      <nav className="space-y-2">
        {links.map((link) => {
          const isActive = pathname === link.href;
          return (
            <Link
              key={link.href}
              href={link.href}
              className={`flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors ${
                isActive
                  ? "bg-primary text-primary-foreground hover:bg-primary/90"
                  : "text-muted-foreground hover:bg-muted hover:text-foreground"
              }`}
            >
              {link.icon}
              {link.title}
            </Link>
          );
        })}
      </nav>
    </aside>
  );
}
