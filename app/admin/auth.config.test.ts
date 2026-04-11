import { describe, it, expect } from "vitest";
import { authConfig } from "./auth.config";

// Mock NextAuth/Request types minimally for what we use
function createMockRequest(pathname: string, isLoggedIn: boolean = false) {
  const url = new URL(`http://localhost:3000${pathname}`);
  return {
    auth: isLoggedIn ? { user: { id: "1", email: "test@example.com" } } : null,
    request: {
      nextUrl: url,
      url: url.toString(),
    },
  };
}

describe("authConfig.callbacks.authorized", () => {
  const { authorized } = authConfig.callbacks!;

  it("should allow public access to login page", async () => {
    const context = createMockRequest("/login", false);
    // @ts-expect-error - authorized signature in test mock
    const result = await authorized(context);
    expect(result).toBe(true);
  });

  it("should redirect logged-in user from login page to dashboard", async () => {
    const context = createMockRequest("/login", true);
    // @ts-expect-error - authorized signature in test mock
    const result = await authorized(context);

    // Check for redirect response
    expect(result).toBeInstanceOf(Response);
    expect((result as Response).status).toBe(302);
    expect((result as Response).headers.get("Location")).toBe("http://localhost:3000/");
  });

  it("should redirect unauthenticated user from dashboard to login", async () => {
    const context = createMockRequest("/", false);
    // @ts-expect-error - authorized signature in test mock
    const result = await authorized(context);
    expect(result).toBe(false);
  });

  it("should allow authenticated user on dashboard", async () => {
    const context = createMockRequest("/", true);
    // @ts-expect-error - authorized signature in test mock
    const result = await authorized(context);
    expect(result).toBe(true);
  });

  it("should allow authenticated user on other protected routes", async () => {
    const context = createMockRequest("/users", true);
    // @ts-expect-error - authorized signature in test mock
    const result = await authorized(context);
    expect(result).toBe(true);
  });

  it("should block unauthenticated user on user show page", async () => {
    const context = createMockRequest("/users/clzxyz123", false);
    // @ts-expect-error - authorized signature in test mock
    const result = await authorized(context);
    expect(result).toBe(false);
  });

  it("should allow authenticated user on user show page", async () => {
    const context = createMockRequest("/users/clzxyz123", true);
    // @ts-expect-error - authorized signature in test mock
    const result = await authorized(context);
    expect(result).toBe(true);
  });
});
