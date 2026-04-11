# Mono Analytics Strategy

This document is the **single source of truth** for how we define, name, and implement product analytics events in Mono via PostHog.

> [!IMPORTANT]
> PostHog event storage is **write-only**. You cannot rename or edit existing events. Follow the naming conventions carefully.

---

## Naming Convention

All event names follow the PostHog-recommended **`[object] [verb]`** format (lowercase, space-separated).

- ✅ `user signed up`, `book created`, `manuscript edited`
- ❌ `user_signed_up`, `BookCreated`, `clicked button`

---

## North Star Metric

### Weekly Active Writers (WAW)

> A user who fires `manuscript edited` on at least **2 distinct days** in a rolling 7-day window.

`manuscript edited` is debounced — it fires once per editor session on first meaningful doc change. A user must open the editor and write on two separate days to count as active.

**Why writing only?** A user who only requests AI feedback (e.g., pastes from Google Docs, gets a critique, and leaves) is a _reviewer_, not a _writer_. `feedback requested` is tracked separately as an engagement quality metric, not a WAW qualifier.

**Measurement:** Computed in PostHog using a Trends insight filtered to unique users performing `manuscript edited`, grouped by week.

---

## Activation

### Definition

> A user is **activated** when they have requested AI editorial feedback on **at least 3 distinct occasions**.

**Why 3?** The first request is curiosity. The second is interest. The third is a workflow. At that point the user understands Mono's unique value — AI-powered editorial feedback — and is likely to return.

### Activation Funnel

| Step | Event                           | Notes                                                 |
| ---- | ------------------------------- | ----------------------------------------------------- |
| 1    | `user signed up`                | Top of funnel                                         |
| 2    | `book created`                  | Setup intent — includes `source: "blank" \| "import"` |
| 3    | `manuscript edited`             | First real writing session                            |
| 4    | `feedback requested`            | First use of AI editorial feedback                    |
| 5    | `feedback requested` (×3 total) | Activation threshold                                  |

---

## Event Schema

### Lifecycle Events

| Event             | Side   | Properties      | Notes                                                                                |
| ----------------- | ------ | --------------- | ------------------------------------------------------------------------------------ |
| `user signed up`  | Server | —               | Fires once at account creation. `$set: { email }` is attached automatically.         |
| `email verified`  | Server | —               | Fires when a user verifies their email address.                                      |
| `user identified` | Client | `email`, `name` | Handled by `PostHogIdentify` component via `posthog.identify()`. Not a custom event. |

### Content Creation Events

| Event             | Side   | Properties                                                                      | Notes                                                                      |
| ----------------- | ------ | ------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `book created`    | Server | `source: "blank" \| "import"`, `initial_word_count?: number`, `book_id: string` | High-intent action. `initial_word_count` is set for imports (0 for blank). |
| `chapter created` | Client | `book_id: string`                                                               | Structural action in the manuscript editor.                                |
| `scene created`   | Client | `book_id: string`                                                               | Structural action in the manuscript editor.                                |

### Writing Events

| Event               | Side   | Properties        | Notes                                                                                                        |
| ------------------- | ------ | ----------------- | ------------------------------------------------------------------------------------------------------------ |
| `manuscript edited` | Client | `book_id: string` | **Debounced.** Fires once per editor session on first meaningful doc change. Represents a "writing session." |

### AI Feedback Events

| Event                | Side   | Properties                                                                           | Notes                                                                  |
| -------------------- | ------ | ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| `feedback requested` | Client | `scope_type: "scene" \| "chapter"`, `trigger: "auto" \| "manual"`, `book_id: string` | Fires when the analysis-ingest plugin submits content for AI review.   |
| `feedback displayed` | Client | `scope_type: "scene" \| "chapter"`, `highlight_count: number`                        | Fires when the AI assistant sidebar is opened and feedback is visible. |

### Navigation & Engagement Events

| Event                      | Side   | Properties         | Notes                                                               |
| -------------------------- | ------ | ------------------ | ------------------------------------------------------------------- |
| `compendium entry created` | Server | —                  | Tracks world-building engagement.                                   |
| `series created`           | Server | —                  | Tracks multi-book organization.                                     |
| `welcome tour completed`   | Client | `skipped: boolean` | Tracks onboarding completion. `skipped` is true if dismissed early. |

### Global Properties

Whenever contextually available, attach these properties to any event:

| Property  | Type     | Notes                                                                                                                                          |
| --------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `book_id` | `string` | The database ID of the book/manuscript. Enables filtering insights per book (e.g., "do users activate faster on their first book or second?"). |

---

## Implementation Details

### Architecture

Events are tracked through two typed helper functions that ensure compile-time safety:

| Layer      | File                                      | Function                                     |
| ---------- | ----------------------------------------- | -------------------------------------------- |
| **Client** | `app/web/lib/analytics/posthog-client.ts` | `trackClientEvent(event)`                    |
| **Server** | `app/web/lib/analytics/posthog-events.ts` | `trackServerEvent(distinctId, email, event)` |

Both functions gracefully degrade in development (no PostHog token) by logging simulated events to the console.

### User Identification

- **Where:** `app/web/components/posthog-identify.tsx`
- **When:** On `useSession()` state change (authenticated → identify, unauthenticated → reset).
- **Distinct ID:** Database user ID (`session.user.id`).
- **Person Properties:** `email`, `name`.

### Editor Analytics Plugin

The `analyticsPlugin` in `app/web/components/editor/index.tsx` handles writing events:

1. Listens for ProseMirror transactions with `analytics` metadata (e.g., `chapter created`, `scene created`).
2. Falls back to a one-time `manuscript edited` event on first meaningful doc change per editor session.

### Debouncing Strategy

- **`manuscript edited`**: Fires once per editor mount (uses a ref flag). Resets when the user navigates to a different document.
- **`feedback requested`**: Fires on each analysis-ingest submission — the ingest plugin already handles its own debouncing (10s idle + deduplication via SHA-256 hash).

---

## Insights to Build

### Dashboard: "Mono Health"

| Insight                 | Type      | Description                                                                          |
| ----------------------- | --------- | ------------------------------------------------------------------------------------ |
| **WAW (North Star)**    | Trend     | Unique users with `manuscript edited`, grouped by week.                              |
| **Activation Funnel**   | Funnel    | `user signed up` → `book created` → `manuscript edited` → `feedback requested` (×3). |
| **Signup → First Edit** | Funnel    | `user signed up` → `manuscript edited`. Conversion time matters.                     |
| **Feedback Adoption**   | Trend     | `feedback requested` count over time, split by `trigger` (auto vs manual).           |
| **Retention**           | Retention | Baseline: `user signed up`. Return event: `manuscript edited`. Timeframe: 7 days.    |

---

## Prioritized Implementation Roadmap

### ✅ Already Implemented

1. `user signed up` — server-side, fires at account creation.
2. `book created` — server-side, fires on new book / import with `source` property.
3. `manuscript edited` — client-side, debounced per editor session.
4. `chapter created` — client-side, fires via editor plugin metadata.
5. `scene created` — client-side, fires via editor plugin metadata.
6. User identification via `posthog.identify()`.
7. `feedback requested` — Wired into the analysis-ingest plugin.
8. `email verified` — server-side, fires when a user verifies their email address.

### 🔜 Next Up (Activation-Critical)

8. `feedback displayed` — Wired into the AI assistant sidebar component when highlights are rendered.

### 📋 Backlog

9. `compendium entry created` — Tracks world-building engagement.
10. `series created` — Tracks multi-book organization.
11. `welcome tour completed` — Tracks onboarding completion.

---

## Revenue Tracking

Revenue tracking will be added once the subscription/payment flow is implemented. When ready:

1. Capture `subscription purchased` with properties: `plan`, `price`, `currency`, `interval`.
2. Build a Trends insight aggregating `sum(price)` over time.

---

## Retention Strategy

### Baseline Definition

- **Baseline event:** `user signed up`
- **Return event:** `manuscript edited`
- **Timeframe:** Rolling 7-day windows, measured over 30 days.

### What "Good" Looks Like

A flat retention curve at ≥ 20% after day 7 would indicate strong product-market fit for an early-stage writing tool. The goal is to see this stabilize, then grow it.

### Red Flags to Watch

| Signal                                                                       | Meaning                                                                      |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| High `feedback requested`, zero `manuscript edited`                          | Users treat Mono as a utility, not a home.                                   |
| High `manuscript edited`, zero `feedback requested`                          | Users haven't discovered the AI editorial value.                             |
| `book created` with `source: "import"` but no subsequent `manuscript edited` | Import friction — users bring a manuscript but don't continue working on it. |
