import { chromium } from "playwright";
import { env } from "../env.js";
import { storage } from "./storage.js";
import { randomUUID } from "node:crypto";

export interface ChartUrls {
  progressChartUrl: string;
  volumeChartUrl: string;
  heatmapUrl: string;
}

export async function screenshotCharts(bookId: string): Promise<ChartUrls> {
  const secret = env.AGENT_SECRET;
  if (!secret) {
    throw new Error("AGENT_SECRET is not set");
  }

  const baseUrl = env.NEXT_PUBLIC_APP_URL;
  const targetUrl = `${baseUrl}/internal/render-charts/${bookId}?token=${secret}`;

  console.log(`[screenshot-charts] Launching headless browser for Book ${bookId}...`);
  // Note: On Railway/Docker, you may need some specific Chromium arguments
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();
    // Use a large viewport to guarantee the wrappers stack cleanly and aren't squished
    await page.setViewportSize({ width: 1200, height: 2000 });

    page.on("console", (msg) => console.log(`[Browser Console] ${msg.text()}`));
    page.on("pageerror", (err) => console.error(`[Browser Error] ${err.message}`));

    console.log(`[screenshot-charts] Navigating to ${targetUrl}`);
    // networkidle ensures that all images, fonts, and async UI mounts complete
    const response = await page.goto(targetUrl, { waitUntil: "networkidle" });

    if (!response?.ok()) {
      throw new Error(`Failed to load charts page. Status: ${response?.status()}`);
    }

    // Check if the page rendered our custom error boundaries
    const errorNode = await page.locator("#error").count();
    if (errorNode > 0) {
      const errorText = await page.locator("#error").textContent();
      throw new Error(`Chart page rendered an error: ${errorText}`);
    }

    // Wait to guarantee Recharts entrance animations have fully settled
    console.log(`[screenshot-charts] Waiting 1500ms for Recharts animations...`);
    await page.waitForTimeout(1500);

    console.log(`[screenshot-charts] Capturing UI components...`);
    const progressBuffer = await page.locator("#book-progress-chart-wrapper").screenshot();
    const volumeBuffer = await page.locator("#daily-volume-chart-wrapper").screenshot();
    const heatmapBuffer = await page.locator("#activity-heatmap-wrapper").screenshot();

    const uploadBuffer = async (buffer: Buffer, name: string) => {
      const path = `analytics-charts/${bookId}/${randomUUID()}-${name}.png`;
      const { url, method } = await storage.getPresignedPutUrl(path, "image/png");

      const response = await fetch(url, {
        method,
        body: buffer,
        headers: {
          "Content-Type": "image/png",
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to upload ${name} to storage: ${response.statusText}`);
      }

      return storage.getPublicUrl(path);
    };

    console.log(`[screenshot-charts] Uploading buffers to storage...`);
    const [progressChartUrl, volumeChartUrl, heatmapUrl] = await Promise.all([
      uploadBuffer(progressBuffer, "progress-chart"),
      uploadBuffer(volumeBuffer, "daily-volume"),
      uploadBuffer(heatmapBuffer, "heatmap"),
    ]);

    console.log(`[screenshot-charts] Success! Images uploaded.`);

    return {
      progressChartUrl,
      volumeChartUrl,
      heatmapUrl,
    };
  } finally {
    await browser.close();
  }
}
