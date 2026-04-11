import { type NextRequest } from "next/server";
import { handlers } from "@/auth";

export const GET = (req: NextRequest) => handlers.GET(req as never);
export const POST = (req: NextRequest) => handlers.POST(req as never);
