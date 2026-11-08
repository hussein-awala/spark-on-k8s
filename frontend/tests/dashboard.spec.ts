import { expect, test, type Page } from "@playwright/test";
import { application, archiveApp, demoApps } from "./data";

async function history(page: Page, items = [archiveApp]) {
  await page.route("**/webserver/api/history", (route) =>
    route.fulfill({ json: { items } }),
  );
}

test("merges history while retaining live status, links, and safe archive actions", async ({
  page,
}) => {
  const live = application();
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({ json: { items: [live], next_cursor: null } }),
  );
  await history(page, [
    application({
      status: "Succeeded",
      source: "history",
      spark_history_proxy: true,
    }),
    archiveApp,
  ]);
  await page.goto("/tests/fixture.html");
  const liveRow = page.getByRole("row").filter({
    has: page.getByRole("button", { name: live.name!, exact: true }),
  });
  await expect(liveRow.getByText("Running", { exact: true })).toBeVisible();
  await expect(
    liveRow.getByRole("link", { name: /^Logs for / }),
  ).toHaveAttribute(
    "href",
    "/nested/spark/webserver/logs/analytics/report-1?tail=1000",
  );
  await expect(
    liveRow.getByRole("link", { name: /^History for / }),
  ).toHaveAttribute(
    "href",
    "/nested/spark/webserver/ui-history/history/report-1",
  );
  await expect(
    page.getByText("Showing 2 of 2 loaded applications"),
  ).toBeVisible();
  const archive = page.getByRole("row").filter({
    has: page.getByRole("button", { name: "Weekly archive", exact: true }),
  });
  await expect(archive.getByText("Archived · namespace unknown")).toBeVisible();
  await expect(archive.getByRole("link", { name: /^Logs for / })).toHaveCount(
    0,
  );
  await archive
    .getByRole("button", { name: "Weekly archive", exact: true })
    .click();
  await expect(
    page
      .getByRole("dialog")
      .getByRole("button", { name: /Stop application|Delete application/ }),
  ).toHaveCount(0);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
});

test("pagination and refresh preserve the loaded scope, search, and status filter", async ({
  page,
}) => {
  const firstPage = Array.from({ length: 50 }, (_, index) =>
    application({ app_id: `app-${index}`, name: `Workload ${index}` }),
  );
  const last = application({
    app_id: "late-job",
    name: "Late failed job",
    status: "Failed",
  });
  const requested: (string | null)[] = [];
  await page.route(/\/webserver\/api\/apps\?/, (route) => {
    const query = new URL(route.request().url()).searchParams;
    expect(query.get("limit")).toBe("50");
    requested.push(query.get("continue_from"));
    return route.fulfill({
      json: query.has("continue_from")
        ? { items: [last], next_cursor: null }
        : { items: firstPage, next_cursor: "page-two" },
    });
  });
  await page.goto("/tests/fixture.html?history=false");
  await page.getByRole("button", { name: "Load more", exact: true }).click();
  await expect(
    page.getByText("Showing 51 of 51 loaded applications"),
  ).toBeVisible();
  await page
    .getByRole("searchbox", { name: "Search loaded applications" })
    .fill("late");
  await page
    .getByRole("combobox", { name: "Filter by status" })
    .selectOption("Failed");
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Late failed job", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("searchbox", { name: "Search loaded applications" }),
  ).toHaveValue("late");
  await expect(
    page.getByRole("combobox", { name: "Filter by status" }),
  ).toHaveValue("Failed");
  await expect(
    page.getByText("Showing 1 of 51 loaded applications"),
  ).toBeVisible();
  await expect
    .poll(() => requested)
    .toEqual([null, "page-two", null, "page-two"]);
});

test("live errors can be retried and history errors do not hide live workloads", async ({
  page,
}) => {
  let appAttempts = 0;
  let historyAttempts = 0;
  await page.route(/\/webserver\/api\/apps\?/, (route) => {
    appAttempts += 1;
    return appAttempts === 1
      ? route.fulfill({
          status: 503,
          json: {
            detail: {
              code: "unavailable",
              message: "Kubernetes is temporarily unavailable",
            },
          },
        })
      : route.fulfill({ json: { items: [application()], next_cursor: null } });
  });
  await page.route("**/webserver/api/history", (route) => {
    historyAttempts += 1;
    return historyAttempts === 1
      ? route.fulfill({
          status: 502,
          json: {
            detail: { code: "upstream", message: "History server timed out" },
          },
        })
      : route.fulfill({ json: { items: [archiveApp] } });
  });
  await page.goto("/tests/fixture.html");
  await expect(page.getByRole("alert")).toContainText(
    "Kubernetes is temporarily unavailable",
  );
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Daily revenue report", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("History server timed out", { exact: false }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Retry history", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Weekly archive", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("History server timed out", { exact: false }),
  ).toHaveCount(0);
});

test("namespace changes keep URL and sidebar consistent and reject stale responses", async ({
  page,
}) => {
  let release: () => void = () => {};
  const firstRequest = new Promise<void>((resolve) => {
    release = resolve;
  });
  let firstStarted = false;
  await page.route(/\/webserver\/api\/apps\?/, async (route) => {
    const namespace = new URL(route.request().url()).searchParams.get(
      "namespace",
    );
    if (namespace === "analytics") {
      firstStarted = true;
      await firstRequest;
    }
    await route
      .fulfill({
        json: {
          items: [
            application({
              app_id: namespace!,
              name: `${namespace} workload`,
              namespace,
            }),
          ],
          next_cursor: null,
        },
      })
      .catch(() => {});
  });
  await page.goto("/tests/fixture.html?history=false&keep=this");
  await expect.poll(() => firstStarted).toBe(true);
  await page
    .getByRole("textbox", { name: "Namespace", exact: true })
    .fill("warehouse");
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "warehouse workload", exact: true }),
  ).toBeVisible();
  release();
  await expect(page).toHaveURL(/keep=this.*namespace=warehouse/);
  await expect(
    page
      .getByRole("navigation")
      .getByRole("link", { name: "Applications", exact: true }),
  ).toHaveAttribute("href", "/nested/spark/webserver/apps?namespace=warehouse");
  await expect(
    page.getByRole("button", { name: "analytics workload", exact: true }),
  ).toHaveCount(0);
});

test("destructive actions confirm requests without claiming delayed deletion has completed", async ({
  page,
}) => {
  let deleted = false;
  let listRequests = 0;
  let attempts = 0;
  let release: () => void = () => {};
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route(/\/webserver\/api\/apps\?/, (route) => {
    listRequests += 1;
    return route.fulfill({
      json: { items: deleted ? [] : [application()], next_cursor: null },
    });
  });
  await page.route("**/app/analytics/report-1", async (route) => {
    expect(route.request().method()).toBe("DELETE");
    attempts += 1;
    if (attempts === 1)
      return route.fulfill({
        status: 403,
        json: {
          detail: { code: "forbidden", message: "Deletion is not permitted" },
        },
      });
    await pending;
    await route.fulfill({ status: 204, body: "" });
  });
  await page.goto("/tests/fixture.html?history=false");
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Delete application", exact: true })
    .click();
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(attempts).toBe(0);
  await expect(
    page.getByRole("dialog", { name: "Daily revenue report", exact: true }),
  ).toBeVisible();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Delete application", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Delete application", exact: true })
    .click();
  await expect(page.getByRole("alert")).toContainText(
    "Deletion is not permitted",
  );
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Delete application", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Working…", exact: true }),
  ).toBeDisabled();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toBeVisible();
  expect(attempts).toBe(2);
  release();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(
    page.getByText("Deletion requested for Daily revenue report."),
  ).toBeVisible();
  await expect.poll(() => listRequests).toBe(2);
  await expect(
    page.getByRole("button", { name: "Daily revenue report", exact: true }),
  ).toBeVisible();
  expect(attempts).toBe(2);
  // Kubernetes can still list a driver after accepting its deletion.
  // Only a later authoritative refresh establishes that it has disappeared.
  deleted = true;
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Daily revenue report", exact: true }),
  ).toHaveCount(0);
});

test("unidentified pods and read-only configuration never expose management controls", async ({
  page,
}) => {
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({
      json: {
        items: [
          application({
            name: "Unidentified driver",
            driver_logs: false,
            spark_ui_proxy: false,
          }),
        ],
        next_cursor: null,
      },
    }),
  );
  await page.goto("/tests/fixture.html?history=false");
  await page
    .getByRole("button", { name: "Unidentified driver", exact: true })
    .click();
  await expect(
    page
      .getByRole("dialog")
      .getByRole("button", { name: /Stop application|Delete application/ }),
  ).toHaveCount(0);
  await page.unroute(/\/webserver\/api\/apps\?/);
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({ json: { items: [application()], next_cursor: null } }),
  );
  await page.goto(
    "/tests/fixture.html?history=false&allowKill=false&allowDelete=false",
  );
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await expect(
    page
      .getByRole("dialog")
      .getByRole("button", { name: /Stop application|Delete application/ }),
  ).toHaveCount(0);
});

test("embedded mode uses authorized links and internal log navigation", async ({
  page,
}) => {
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({
      json: {
        items: [application({ spark_history_proxy: true })],
        next_cursor: null,
      },
    }),
  );
  await page.routeWebSocket("**/webserver/ws/logs/**", () => {});
  await page.goto(
    "/tests/fixture.html?history=false&embedded=true&namespaceEditable=false&authorizedLinks=true&contextLabel=Daily%20sales",
  );
  await expect(page.getByRole("complementary")).toHaveCount(0);
  await expect(
    page.getByRole("textbox", { name: "Namespace", exact: true }),
  ).toHaveCount(0);
  await expect(page.getByText("Daily sales", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("link", { name: /^Spark UI for / }),
  ).toHaveAttribute("href", "/authorized/spark-ui?task=aggregate");
  await expect(
    page.getByRole("link", { name: /^History for / }),
  ).toHaveAttribute("href", "/authorized/history?task=aggregate");
  await page.getByRole("button", { name: /^Logs for / }).click();
  await expect(
    page.getByRole("heading", { name: "Application logs." }),
  ).toBeVisible();
  await expect(page).toHaveURL(/tests\/fixture.html/);
  await page.getByRole("button", { name: "Applications", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Applications." }),
  ).toBeVisible();
});

test("dashboard stays usable on narrow screens and exposes keyboard dialog controls", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({ json: { items: demoApps, next_cursor: null } }),
  );
  await history(page);
  await page.goto("/tests/fixture.html");
  const name = page.getByRole("button", {
    name: "customer-daily-rollup",
    exact: true,
  });
  await expect(name).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await name.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(name).toBeFocused();
});

test("empty and filtered lists remain actionable and stopping a driver refreshes its state", async ({
  page,
}) => {
  let items: ReturnType<typeof application>[] = [];
  let stopped = false;
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({ json: { items, next_cursor: null } }),
  );
  await page.route("**/app/analytics/report-1", (route) => {
    expect(route.request().method()).toBe("PATCH");
    stopped = true;
    items = [application({ status: "Failed", spark_ui_proxy: false })];
    return route.fulfill({ status: 204, body: "" });
  });
  await page.goto("/tests/fixture.html?history=false");
  await expect(
    page.getByText("No applications have been found in analytics."),
  ).toBeVisible();
  items = [application()];
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await page
    .getByRole("searchbox", { name: "Search loaded applications" })
    .fill("missing-job");
  await expect(
    page.getByRole("heading", { name: "No matching applications" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Clear filters", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Stop application", exact: true })
    .click();
  expect(stopped).toBe(false);
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Stop application", exact: true })
    .click();
  await expect(
    page.getByText("Stop requested for Daily revenue report."),
  ).toBeVisible();
  await expect(
    page.getByRole("dialog").getByText("Failed", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .getByRole("dialog")
      .getByRole("button", { name: "Stop application", exact: true }),
  ).toHaveCount(0);
});

test("all console navigation items fit when a log page resizes to a narrow screen", async ({
  page,
}) => {
  await page.goto("/tests/fixture.html?view=logs&runs=true&demo=true");
  await expect(page.getByTestId("log-text")).toContainText("SparkContext");
  await page
    .getByRole("button", { name: "Prepare excerpt", exact: true })
    .click();
  await page.keyboard.press("Escape");
  const navigation = page.getByRole("navigation");
  for (const width of [390, 320, 600, 768]) {
    await page.setViewportSize({ width, height: 844 });
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth),
    ).toBeLessThanOrEqual(width);
    for (const name of ["Applications", "Recorded runs"]) {
      const link = navigation.getByRole("link", { name, exact: true });
      await expect(link).toBeInViewport();
      const bounds = (await link.boundingBox())!;
      expect(bounds.x).toBeGreaterThanOrEqual(0);
      expect(bounds.x + bounds.width).toBeLessThanOrEqual(width);
      await link.focus();
      await expect(link).toBeFocused();
    }
    const current = navigation.getByText("Driver logs", { exact: true });
    await expect(current).toBeInViewport();
    const bounds = (await current.boundingBox())!;
    expect(bounds.x + bounds.width).toBeLessThanOrEqual(width);
    expect(await page.evaluate(() => window.scrollX)).toBe(0);
  }
});
