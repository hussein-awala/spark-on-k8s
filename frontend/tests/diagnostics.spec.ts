import { expect, test } from "@playwright/test";
import { application } from "./data";

const finding = {
  code: "image_pull_failed",
  severity: "error",
  resource: "report-1-driver",
  container: "driver",
  summary: "Container image is unavailable",
  evidence: "<img src=x onerror=alert(1)>",
  remediation: "Check the image reference and imagePullSecrets.",
};
const diagnosis = {
  schema_version: 1,
  namespace: "analytics",
  app_id: "report-1",
  driver_pod: "report-1-driver",
  incomplete: true,
  inspected_pods: 2,
  inspected_events: 3,
  findings: [finding],
};

test("details show bounded diagnostic evidence safely under a deployment prefix", async ({
  page,
}) => {
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({
      json: { items: [application({ status: "Pending" })], next_cursor: null },
    }),
  );
  await page.route("**/pods/report-1-driver/diagnostics", (route) => {
    expect(new URL(route.request().url()).pathname).toBe(
      "/nested/spark/webserver/api/apps/analytics/pods/report-1-driver/diagnostics",
    );
    return route.fulfill({ json: { application: application(), diagnosis } });
  });
  await page.goto("/tests/fixture.html?history=false");
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  const section = page.getByRole("region", { name: "Application diagnostics" });
  await expect(section.getByText(finding.summary)).toBeVisible();
  await expect(
    section.getByText(finding.evidence, { exact: true }),
  ).toBeVisible();
  await expect(section.locator("img")).toHaveCount(0);
  await expect(section.getByText("2 pods · 3 events inspected")).toBeVisible();
  await expect(
    section.getByText(/Some evidence was unavailable/),
  ).toBeVisible();
  await expect(
    section.getByText(finding.remediation, { exact: false }),
  ).toBeVisible();
});

test("diagnostic errors can be retried without hiding application details", async ({
  page,
}) => {
  let attempts = 0;
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({ json: { items: [application()], next_cursor: null } }),
  );
  await page.route("**/diagnostics", (route) => {
    attempts += 1;
    return route.fulfill(
      attempts === 1
        ? {
            status: 503,
            json: { detail: { message: "Diagnostics are unavailable" } },
          }
        : {
            json: {
              diagnosis: { ...diagnosis, incomplete: false, findings: [] },
            },
          },
    );
  });
  await page.goto("/tests/fixture.html?history=false");
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await expect(
    page.getByRole("dialog").getByText("report-1-driver", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Diagnostics are unavailable", { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Retry diagnostics" }).click();
  await expect(
    page.getByText("No specific problems observed in this snapshot."),
  ).toBeVisible();
  await expect(
    page.getByText("Diagnostics are unavailable", { exact: true }),
  ).toHaveCount(0);
});

test("closing details aborts diagnostics before opening a different application", async ({
  page,
}) => {
  const second = application({
    app_id: "second",
    name: "Second application",
    driver_pod: "second-driver",
  });
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({
      json: { items: [application(), second], next_cursor: null },
    }),
  );
  await page.route("**/pods/report-1-driver/diagnostics", async (route) => {
    // Keep the response pending until the component cancels the request.
    await new Promise((resolve) => setTimeout(resolve, 500));
    await route.fulfill({ json: { diagnosis } }).catch(() => {});
  });
  await page.route("**/pods/second-driver/diagnostics", (route) =>
    route.fulfill({
      json: {
        diagnosis: {
          ...diagnosis,
          app_id: "second",
          driver_pod: "second-driver",
          findings: [],
          incomplete: false,
        },
      },
    }),
  );
  await page.goto("/tests/fixture.html?history=false");
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await expect(page.getByText("Checking pod states and events…")).toBeVisible();
  const cancelled = page.waitForEvent("requestfailed", {
    predicate: (request) =>
      request.url().includes("report-1-driver/diagnostics"),
  });
  await page.keyboard.press("Escape");
  await cancelled;
  await page
    .getByRole("button", { name: "Second application", exact: true })
    .click();
  await expect(
    page.getByText("No specific problems observed in this snapshot."),
  ).toBeVisible();
  await expect(page.getByText(finding.summary)).toHaveCount(0);
});
