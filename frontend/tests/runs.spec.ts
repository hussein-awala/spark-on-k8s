import { expect, test } from "@playwright/test";

const run = (overrides = {}) => ({
  uid: "uid-first",
  app_id: "daily-job",
  name: "Daily aggregation",
  namespace: "analytics",
  driver_pod: "daily-job-driver",
  image: "spark:4.3.0",
  spark_version: "4.3.0",
  status: "Running",
  created_at: "2026-11-07T08:00:00Z",
  first_seen: "2026-11-07T08:00:01Z",
  last_seen: "2026-11-07T09:00:00Z",
  disappeared_at: "2026-11-07T09:00:30Z",
  ...overrides,
});
const fixture = "/tests/fixture.html?view=runs&runs=true";

test("records remain separate after names are reused and never offer live actions", async ({
  page,
}) => {
  await page.route(/\/runs\/v1\?/, (route) =>
    route.fulfill({
      json: {
        schema_version: 1,
        items: [
          run(),
          run({ uid: "uid-second", status: "Succeeded", disappeared_at: null }),
        ],
        next_cursor: null,
      },
    }),
  );
  await page.goto(fixture);
  await expect(
    page.getByRole("heading", { name: "Recorded runs." }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Daily aggregation", exact: true }),
  ).toHaveCount(2);
  await expect(
    page.getByRole("cell", { name: "No longer observed", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("cell", { name: "Running", exact: true }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Daily aggregation", exact: true })
    .first()
    .click();
  await expect(
    page.getByRole("dialog").getByText("uid-first", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .getByRole("dialog")
      .getByRole("button", { name: /Delete|Stop|Replay/ }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("link", { name: "Logs", exact: true }),
  ).toHaveCount(0);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(
    page.getByRole("link", { name: "Recorded runs", exact: true }),
  ).toHaveAttribute("href", "/nested/spark/webserver/runs?namespace=analytics");
});

test("uses server filters and bound cursors, refreshes without duplicate rows", async ({
  page,
}) => {
  const requests: URLSearchParams[] = [];
  await page.route(/\/runs\/v1\?/, (route) => {
    const query = new URL(route.request().url()).searchParams;
    requests.push(query);
    expect(query.get("limit")).toBe("50");
    const next = query.has("cursor");
    return route.fulfill({
      json: {
        schema_version: 1,
        items: [
          run({
            uid: next ? "uid-second" : "uid-first",
            name: next ? "Second record" : "First record",
          }),
        ],
        next_cursor: next ? null : "bound-cursor",
      },
    });
  });
  await page.goto(fixture);
  await page.getByRole("button", { name: "Load more", exact: true }).click();
  await expect(page.getByText("2 recorded runs loaded")).toBeVisible();
  await expect.poll(() => requests.at(-1)?.get("cursor")).toBe("bound-cursor");
  await page
    .getByRole("combobox", { name: "Last status" })
    .selectOption("Failed");
  await page
    .getByRole("combobox", { name: "Pod observation" })
    .selectOption("missing");
  await expect.poll(() => requests.at(-1)?.get("presence")).toBe("missing");
  await expect.poll(() => requests.at(-1)?.get("status")).toBe("Failed");
  expect(requests.at(-1)?.get("cursor")).toBeNull();
  await page.getByRole("textbox", { name: "Namespace" }).fill("production");
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  await expect.poll(() => requests.at(-1)?.get("namespace")).toBe("production");
  await expect(page.getByText("1 recorded run loaded")).toBeVisible();
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(page.getByText("1 recorded run loaded")).toBeVisible();
  await expect(page.getByRole("combobox", { name: "Last status" })).toHaveValue(
    "Failed",
  );
});

test("shows recoverable errors and keeps untrusted record text inert", async ({
  page,
}) => {
  let calls = 0;
  await page.route(/\/runs\/v1\?/, (route) =>
    ++calls === 1
      ? route.fulfill({
          status: 503,
          json: { error: { message: "Recorder is recovering" } },
        })
      : route.fulfill({
          json: {
            schema_version: 1,
            items: [
              run({ name: '<img src=x onerror="window.compromised=true">' }),
            ],
            next_cursor: null,
          },
        }),
  );
  await page.goto(fixture);
  await expect(page.getByRole("alert")).toContainText("Recorder is recovering");
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(
    page.getByRole("button", {
      name: '<img src=x onerror="window.compromised=true">',
      exact: true,
    }),
  ).toBeVisible();
  expect(await page.evaluate(() => "compromised" in window)).toBe(false);
  await expect(page.locator(".application-table img")).toHaveCount(0);
});

test("disabled history makes no archive request", async ({ page }) => {
  let calls = 0;
  await page.route(/\/runs\/v1\?/, (route) => {
    calls += 1;
    return route.fulfill({
      json: { schema_version: 1, items: [], next_cursor: null },
    });
  });
  await page.goto("/tests/fixture.html?view=runs&runs=false");
  await expect(
    page.getByRole("heading", { name: "Run history is not available" }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "Recorded runs", exact: true }),
  ).toHaveCount(0);
  expect(calls).toBe(0);
});

test("mobile run history stays within the page and supports empty results", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.route(/\/runs\/v1\?/, (route) =>
    route.fulfill({
      json: { schema_version: 1, items: [], next_cursor: null },
    }),
  );
  await page.goto(fixture);
  await expect(
    page.getByRole("heading", { name: "No recorded runs" }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  for (const name of ["Last status", "Pod observation"]) {
    const control = page.getByRole("combobox", { name });
    const box = await control.boundingBox();
    expect(box).not.toBeNull();
    expect(box!.x + box!.width).toBeLessThanOrEqual(374);
    await expect(control).toBeInViewport();
  }
});

test("a slow older filter cannot replace the current records", async ({
  page,
}) => {
  await page.route(/\/runs\/v1\?/, async (route) => {
    const status = new URL(route.request().url()).searchParams.get("status");
    if (status === "Running")
      await new Promise((resolve) => setTimeout(resolve, 250));
    await route
      .fulfill({
        json: {
          schema_version: 1,
          items: [
            run({
              name:
                status === "Running"
                  ? "Stale running record"
                  : "Current record",
            }),
          ],
          next_cursor: null,
        },
      })
      .catch(() => {});
  });
  await page.goto(fixture);
  await expect(
    page.getByRole("button", { name: "Current record", exact: true }),
  ).toBeVisible();
  await page
    .getByRole("combobox", { name: "Last status" })
    .selectOption("Running");
  await page
    .getByRole("combobox", { name: "Last status" })
    .selectOption("Failed");
  await expect(
    page.getByRole("button", { name: "Current record", exact: true }),
  ).toBeVisible();
  await page.waitForTimeout(350);
  await expect(
    page.getByRole("button", { name: "Stale running record", exact: true }),
  ).toHaveCount(0);
});

test("retrying a failed next page retains the prior records and cursor", async ({
  page,
}) => {
  let attempts = 0;
  await page.route(/\/runs\/v1\?/, (route) => {
    const next = new URL(route.request().url()).searchParams.get("cursor");
    if (next && ++attempts === 1)
      return route.fulfill({
        status: 503,
        json: { detail: { message: "Please retry the page" } },
      });
    return route.fulfill({
      json: {
        schema_version: 1,
        items: [
          run({
            uid: next ? "next" : "first",
            name: next ? "Next record" : "Prior record",
          }),
        ],
        next_cursor: next ? null : "page2",
      },
    });
  });
  await page.goto(fixture);
  await page.getByRole("button", { name: "Load more", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Please retry the page");
  await expect(
    page.getByRole("button", { name: "Prior record", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page.getByText("2 recorded runs loaded")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Next record", exact: true }),
  ).toBeVisible();
});
