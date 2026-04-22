# BuyItForLifeScore: Core SEO & AIO Standards

_This document serves as the absolute baseline architectural standard for the frontend. Any deviation from these rules guarantees a degradation in E-E-A-T trust, crawlability, or AI ingestion. Our target is a permanent 100/100 Lighthouse score._

## 1. Global Architectural Mandates

### 1.1 Zero-JavaScript Policy

- **Rule:** The client-side payload must contain **zero** bytes of framework JavaScript (e.g., React, Vue, Svelte runtimes).
- **Rationale:** JavaScript bloats the main thread, delays Total Blocking Time (TBT), and delays Googlebot indexing (which defers JS rendering to a secondary, resource-heavy crawling wave).
- **Execution:** All pages and components must be rendered purely via Astro at build-time. Complex interactions must be CSS-driven (e.g., `:hover`, `:checked` states) or utilize purely native HTML features.

### 1.2 SVG-Only Data Visualizations

- **Rule:** Visual data charts (Radars, Comparisons, Bar Charts) must be constructed utilizing server-side trigonometry and output as raw `<svg>` tags.
- **Rationale:** Avoids massive client-side charting libraries (Chart.js, Recharts, D3) which violate the Zero-JavaScript policy and crush mobile performance scores.
- **Execution:** See `src/components/RadarChart.astro` for the mathematical implementation.

### 1.3 The "Invisible Markdown Feeder" (AIO Strategy)

- **Rule:** Every visual data chart _must_ be immediately followed by a `<table class="sr-only">`.
- **Rationale:** AI Scrapers (SearchGPT, Claude, OpenAI) strip CSS and SVGs when converting a DOM to Markdown. A visually hidden table using `.sr-only` ensures the raw data is fed directly to the scraper as a Markdown table.
- **SEO Safety:** Because `.sr-only` utilizes Screen Reader clipping (`clip: rect(0,0,0,0); overflow: hidden;`), it is officially recognized by Web Content Accessibility Guidelines (WCAG) and prevents Google penalties for "Blackhat Hidden Text".

### 1.4 Deep Semantic HTML

- **Rule:** The DOM must read like a well-formatted academic paper.
- **Execution:**
  - Strictly **one** `<h1>` per page.
  - Content MUST be grouped inside proper `<section>` and `<article>` tags.
  - Avoid generic `<div>` soup. Use `<aside>` for supplementary data and `<nav>` for breadcrumbs.

---

## 2. JSON-LD Structured Data Mandates

_Every single page must inject a highly targeted `application/ld+json` script into the `<head>`._

### 2.1 The Homepage (`/`)

- **Schema:** `WebSite` or `Dataset`
- **Goal:** Establish the site as an authoritative data hub and provide a global aggregation summary.

### 2.2 Global Hubs (`/category`, `/department`)

- **Schema:** `CollectionPage` or `ItemList`
- **Goal:** Guide the crawler safely into the deep spokes without hitting crawl-budget dead ends. Emphasizes the hierarchy of the Head Terms.

### 2.3 Brand Pages (`/brands/[brand]`)

- **Schema:** `Brand` (sometimes nested within a `CollectionPage`)
- **Goal:** Establish entity recognition. We are an authority reviewing the entity (the Brand).

### 2.4 Product Line / Ecosystem Pages (`/product/[brand]/[line]`)

- **Schema:** `ProductGroup` or `ItemList` (of models)
- **Goal:** Aggregate generic reviews. Explicitly group all specific models to avoid cannibalization of the specific Model pages.

### 2.5 Specific Product Models (`/product/[brand]/[line]/[model]`)

- **Schema:** `Product` with `AggregateRating`
- **Rule:** The most important schema on the site.
- **Execution:**
  - Must contain `ratingValue` (e.g., 85).
  - Must contain `bestRating` (e.g., 100).
  - Must contain `ratingCount` (Total mentions).
  - **Goal:** Triggers the native "Star Ratings" and review counts inside standard Google Search Results (Rich Snippets), drastically increasing Click-Through Rate (CTR).

---

## 3. SEO Edge Cases & Crawl Budget

- **Standalone Products:** If a product does not belong to a Product Line, it must still route to the exact same `[model].astro` template to ensure it receives the exact same JSON-LD `Product` schema as tiered models. Do not orphan products.
- **CSS Variable Styling:** Rely on CSS Variable (`:root`) driven styling. By passing simple HTML elements rather than deep utility-class strings (like Tailwind), the total DOM size (bytes transmitted) shrinks dramatically, decreasing Time to First Byte (TTFB).
