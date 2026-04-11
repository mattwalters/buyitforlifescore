import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { trackRedditEvent } from "./reddit-pixel";

describe("trackRedditEvent", () => {
  beforeEach(() => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubGlobal("window", {
      // Create a mock rdt function that mimics what the Reddit script does
      // We don't define it here initially to test the fallback logic
      rdt: undefined,
    });
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("should not call window.rdt in development", () => {
    vi.stubEnv("NODE_ENV", "development");
    const consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    trackRedditEvent("SignUp");

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((window.rdt as any)?.callQueue).toBeUndefined();
    expect(consoleLogSpy).toHaveBeenCalledWith("[Reddit Pixel] Simulated track event: SignUp", "");
    consoleLogSpy.mockRestore();
  });

  it("should create a stub window.rdt and push to callQueue if undefined", () => {
    trackRedditEvent("Purchase");

    // The function should have been created
    expect(typeof window.rdt).toBe("function");

    // It should have pushed the arguments into the callQueue
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const queue = (window.rdt as any).callQueue;
    expect(queue).toBeDefined();
    expect(queue.length).toBe(1);
    expect(queue[0]).toEqual(["track", "Purchase"]);
  });

  it("should push eventName and eventData to callQueue if undefined", () => {
    trackRedditEvent("Purchase", { value: 10, currency: "USD" });

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const queue = (window.rdt as any).callQueue;
    expect(queue[0]).toEqual(["track", "Purchase", { value: 10, currency: "USD" }]);
  });

  it("should not throw if window or window.rdt is undefined in production", () => {
    vi.stubGlobal("window", undefined);
    expect(() => trackRedditEvent("SignUp")).not.toThrow();

    vi.stubGlobal("window", {});
    expect(() => trackRedditEvent("SignUp")).not.toThrow();
  });
});
