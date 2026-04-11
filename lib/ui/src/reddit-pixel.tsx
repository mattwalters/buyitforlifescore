"use client";

import { usePathname, useSearchParams } from "next/navigation";
import Script from "next/script";
import { useEffect, Suspense } from "react";

// Ensure typescript knows about the global rdt function
declare global {
  interface Window {
    rdt?: (...args: unknown[]) => void;
  }
}

/**
 * Safely tracks a Reddit Pixel event.
 * It will only fire if `window.rdt` is defined and if we are in a production environment.
 */
export function trackRedditEvent(
  eventName:
    | "PageVisit"
    | "SignUp"
    | "Purchase"
    | "AddToCart"
    | "ViewContent"
    | "Search"
    | "Lead"
    | "CustomEvent",
  eventData?: Record<string, unknown>,
) {
  if (process.env.NODE_ENV !== "production") {
    // Optionally log in development for debugging.
    // We use console.log instead of console.debug because debug is hidden by default in Chrome.
    console.log(`[Reddit Pixel] Simulated track event: ${eventName}`, eventData || "");
    return;
  }

  if (typeof window !== "undefined") {
    // Skip if on localhost
    if (window.location?.hostname === "localhost" || window.location?.hostname === "127.0.0.1") {
      console.log(`[Reddit Pixel] Bypassed on localhost: ${eventName}`, eventData || "");
      return;
    }

    // If window.rdt doesn't exist yet, define the exact stub that Reddit's snippet uses.
    // This allows us to safely queue events BEFORE the actual script executes.
    if (!window.rdt) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const p = (window.rdt = function (...args: any[]) {
        p.callQueue = p.callQueue || [];
        p.callQueue.push(args);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any);
      p.callQueue = [];
    }

    if (eventData) {
      window.rdt!("track", eventName, eventData);
    } else {
      window.rdt!("track", eventName);
    }
  }
}

function RedditPixelContent() {
  const pathname = usePathname();
  const searchParams = useSearchParams();

  useEffect(() => {
    // 1. Capture Click ID (rdt_cid) from URL if present
    const rdtCid = searchParams.get("rdt_cid");
    if (rdtCid) {
      // Set cookie for 30 days
      const thirtyDays = 30 * 24 * 60 * 60;
      let domainStr = "";

      // Due to the split-domain strategy (marketing = writemono.com, web = i.writemono.com)
      // we MUST write the tracker to the root domain so that the web app backend can read it.
      if (typeof window !== "undefined" && window.location.hostname) {
        const hostname = window.location.hostname;
        if (hostname !== "localhost" && hostname !== "127.0.0.1") {
          const parts = hostname.split(".");
          // Get the last two parts of the domain (e.g., this is super naive but works for standard .coms)
          const rootDomain = parts.length > 2 ? parts.slice(-2).join(".") : hostname;
          domainStr = ` domain=.${rootDomain};`;
        }
      }

      document.cookie = `reddit_click_id=${encodeURIComponent(rdtCid)}; max-age=${thirtyDays}; path=/;${domainStr}`;
      if (process.env.NODE_ENV !== "production") {
        console.log(`[Reddit Pixel] Captured click ID (rdt_cid): ${rdtCid}`);
      }
    }

    trackRedditEvent("PageVisit");

    // Conversion tracking via cookies
    // Server actions write "reddit_pending_conversion=EventName" before redirect
    const cookies = document.cookie.split(";");
    for (let i = 0; i < cookies.length; i++) {
      const cookie = cookies[i].trim();
      if (cookie.startsWith("reddit_pending_conversion=")) {
        const rawValue = cookie.substring("reddit_pending_conversion=".length);
        const value = decodeURIComponent(rawValue);
        const parts = value.split("|");
        const eventName = parts[0] as Parameters<typeof trackRedditEvent>[0];
        const conversionId = parts[1];

        if (process.env.NODE_ENV !== "production") {
          console.log(`[Reddit Pixel] Found pending conversion cookie for: ${eventName}`);
        }
        if (conversionId) {
          trackRedditEvent(eventName, { conversionId });
        } else {
          trackRedditEvent(eventName);
        }
        // Immediately delete the cookie so it doesn't fire again on refresh
        document.cookie =
          "reddit_pending_conversion=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;";
        break;
      }
    }
  }, [pathname, searchParams]);

  return null;
}

export function RedditPixel() {
  const isProd = process.env.NODE_ENV === "production";

  return (
    <>
      <Suspense fallback={null}>
        <RedditPixelContent />
      </Suspense>
      {isProd && (
        <Script
          id="reddit-pixel"
          strategy="afterInteractive"
          dangerouslySetInnerHTML={{
            __html: `
            if (window.location.hostname !== "localhost" && window.location.hostname !== "127.0.0.1") {
              !function(w,d){if(!w.rdt){var p=w.rdt=function(){p.sendEvent?p.sendEvent.apply(p,arguments):p.callQueue.push(arguments)};p.callQueue=[];var t=d.createElement("script");t.src="https://www.redditstatic.com/ads/pixel.js?pixel_id=a2_iji1bn6jft0b",t.async=!0;var s=d.getElementsByTagName("script")[0];s.parentNode.insertBefore(t,s)}}(window,document);
              rdt('init','a2_iji1bn6jft0b');
            }
            `,
          }}
        />
      )}
    </>
  );
}
