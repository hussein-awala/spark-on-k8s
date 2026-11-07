import { readFile } from "node:fs/promises";
import {
  expect,
  test,
  type Page,
  type Request,
  type Route,
  type WebSocketRoute,
} from "@playwright/test";
import { application } from "./data";
import type { TaskProps } from "./airflow-fixture";

const bundle = new URL(
  "../../spark_on_k8s/airflow/ui/static/airflow.js",
  import.meta.url,
);
const stylesheet = new URL(
  "../../spark_on_k8s/airflow/ui/static/console.css",
  import.meta.url,
);
const capabilities = {
  namespace: "analytics",
  can_kill: false,
  can_delete: false,
  spark_ui_url: "https://spark.example.test/webserver/ui/analytics/report-1",
  spark_history_url:
    "https://spark.example.test/webserver/ui-history/history/report-1",
};

async function builtPlugin(page: Page) {
  // Exercise the shipped IIFE against the host's React, not a source-only test build.
  const [javascript, css] = await Promise.all([
    readFile(bundle),
    readFile(stylesheet),
  ]);
  await page.route("**/spark-on-k8s/static/airflow.js", (route) =>
    route.fulfill({ contentType: "text/javascript", body: javascript }),
  );
  await page.route("**/spark-on-k8s/static/console.css", (route) =>
    route.fulfill({ contentType: "text/css", body: css }),
  );
}

async function api(
  page: Page,
  overrides: {
    capabilities?: typeof capabilities;
    handle?: (route: Route) => Promise<boolean>;
  } = {},
) {
  const requests: Request[] = [];
  await page.route("**/spark-on-k8s/api/**", async (route) => {
    requests.push(route.request());
    if (await overrides.handle?.(route)) return;
    const pathname = new URL(route.request().url()).pathname;
    if (pathname.endsWith("/capabilities"))
      return route.fulfill({ json: overrides.capabilities || capabilities });
    if (pathname.endsWith("/history"))
      return route.fulfill({
        json: {
          items: [
            application({
              source: "history",
              status: "Succeeded",
              spark_history_proxy: true,
            }),
          ],
        },
      });
    if (pathname.endsWith("/application"))
      return route.fulfill({ json: application() });
    await route.fulfill({
      status: 404,
      json: { detail: "Unexpected plugin endpoint" },
    });
  });
  return requests;
}

function queryOf(request: Request) {
  return Object.fromEntries(new URL(request.url()).searchParams);
}

test.beforeEach(async ({ page }) => {
  await builtPlugin(page);
});

test("native bundle shares host React and binds every request to the task under an Airflow prefix", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const requests = await api(page);
  const params = new URLSearchParams({
    base: "/teams/production/airflow/",
    dagId: "sales/daily & regional",
    runId: "manual__2026-11-07T09:00:00+00:00",
    taskId: "group.aggregate?part=1",
    mapIndex: "3",
    tryNumber: "4",
  });
  await page.goto(`/tests/airflow-fixture.html?${params}`);
  await expect(
    page.getByRole("button", { name: "Daily revenue report", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("complementary")).toHaveCount(0);
  await expect(
    page.getByRole("textbox", { name: "Namespace", exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByText("Showing 1 of 1 loaded applications"),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: /^Spark UI for / }),
  ).toHaveAttribute("href", capabilities.spark_ui_url);
  await expect(
    page.getByRole("link", { name: /^History for / }),
  ).toHaveAttribute("href", capabilities.spark_history_url);
  expect(requests.length).toBeGreaterThanOrEqual(3);
  for (const request of requests) {
    expect(new URL(request.url()).pathname).toMatch(
      /^\/teams\/production\/airflow\/spark-on-k8s\/api\//,
    );
    expect(queryOf(request)).toEqual({
      dag_id: "sales/daily & regional",
      run_id: "manual__2026-11-07T09:00:00+00:00",
      task_id: "group.aggregate?part=1",
      map_index: "3",
      try_number: "4",
    });
  }
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await expect(
    page
      .getByRole("dialog")
      .getByRole("button", { name: /Stop application|Delete application/ }),
  ).toHaveCount(0);
  expect(errors).toEqual([]);
});

test("region-aware Airflow tasks use region coordinates instead of a conflicting map index", async ({
  page,
}) => {
  const requests = await api(page);
  await page.goto(
    "/tests/airflow-fixture.html?regionId=task-group%2Fbranch&regionIndex=7&mapIndex=99",
  );
  await expect(
    page.getByRole("button", { name: "Daily revenue report", exact: true }),
  ).toBeVisible();
  expect(requests.length).toBeGreaterThanOrEqual(2);
  for (const request of requests) {
    expect(queryOf(request)).toMatchObject({
      region_id: "task-group/branch",
      region_index: "7",
    });
    expect(new URL(request.url()).searchParams.has("map_index")).toBe(false);
  }
});

test("missing task context makes no backend calls and capability failures can be retried", async ({
  page,
}) => {
  let failures = 1;
  const requests = await api(page, {
    handle: async (route) => {
      if (
        new URL(route.request().url()).pathname.endsWith("/capabilities") &&
        failures-- > 0
      ) {
        await route.fulfill({
          status: 403,
          json: { detail: "You cannot view this task attempt" },
        });
        return true;
      }
      return false;
    },
  });
  await page.goto("/tests/airflow-fixture.html?missingContext=true");
  await expect(
    page.getByText("Open a task attempt to view its Spark application."),
  ).toBeVisible();
  expect(requests).toHaveLength(0);
  await page.evaluate(() =>
    window.airflowFixture.render({
      dagId: "daily_sales",
      runId: "run-1",
      taskId: "aggregate",
      mapIndex: -1,
      taskInstance: { try_number: 2 },
    }),
  );
  await expect(
    page.getByText("You cannot view this task attempt", { exact: false }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Daily revenue report", exact: true }),
  ).toBeVisible();
});

test("authorized stop and delete retain task coordinates and require confirmed action headers", async ({
  page,
}) => {
  const actions: Request[] = [];
  let deletionAttempts = 0;
  await api(page, {
    capabilities: { ...capabilities, can_kill: true, can_delete: true },
    handle: async (route) => {
      if (!["POST", "DELETE"].includes(route.request().method())) return false;
      actions.push(route.request());
      if (route.request().method() === "DELETE" && ++deletionAttempts === 1)
        await route.fulfill({
          status: 403,
          json: { detail: "Task deletion was denied" },
        });
      else await route.fulfill({ status: 204, body: "" });
      return true;
    },
  });
  await page.goto("/tests/airflow-fixture.html");
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Stop application", exact: true })
    .click();
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(actions).toHaveLength(0);
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Stop application", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Stop application", exact: true })
    .click();
  await expect(
    page.getByText("Stop requested for Daily revenue report."),
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
    "Task deletion was denied",
  );
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Delete application", exact: true })
    .click();
  await expect(
    page.getByText("Daily revenue report was deleted."),
  ).toBeVisible();
  expect(actions.map((request) => request.method())).toEqual([
    "POST",
    "DELETE",
    "DELETE",
  ]);
  expect(new URL(actions[0].url()).pathname).toBe(
    "/airflow/spark-on-k8s/api/kill",
  );
  for (const request of actions) {
    expect(request.headers()["x-spark-on-k8s-action"]).toBe("true");
    expect(queryOf(request)).toEqual({
      dag_id: "daily_sales",
      run_id: "scheduled__2026-11-07T09:00:00+00:00",
      task_id: "aggregate",
      map_index: "-1",
      try_number: "2",
    });
  }
});

test("task changes close old log streams and task-bound logs stay literal", async ({
  page,
}) => {
  const sockets: WebSocketRoute[] = [];
  let closed = 0;
  await api(page);
  await page.routeWebSocket("**/spark-on-k8s/api/logs?*", (route) => {
    sockets.push(route);
    route.onClose(() => {
      closed += 1;
    });
  });
  await page.goto(
    "/tests/airflow-fixture.html?base=%2Fairflow%2F&dagId=sales%2Fdaily",
  );
  await page.getByRole("button", { name: /^Logs for / }).click();
  await expect.poll(() => sockets.length).toBe(1);
  const logQuery = new URL(sockets[0].url()).searchParams;
  expect(logQuery.get("dag_id")).toBe("sales/daily");
  expect(logQuery.get("tail")).toBe("1000");
  expect(logQuery.has("namespace")).toBe(false);
  expect(logQuery.has("app_id")).toBe(false);
  const literal = '<img src=x onerror="window.pluginLogExecuted=true">';
  sockets[0].send(literal);
  await expect(page.getByTestId("log-text")).toHaveText(literal);
  await expect(page.locator(".log-viewport img")).toHaveCount(0);
  const next: TaskProps = {
    dagId: "next_dag",
    runId: "next_run",
    taskId: "next_task",
    mapIndex: -1,
    taskInstance: { try_number: 1 },
  };
  await page.evaluate((props) => window.airflowFixture.render(props), next);
  await expect.poll(() => closed).toBe(1);
  await expect(
    page.getByRole("heading", { name: "Applications." }),
  ).toBeVisible();
  await page.getByRole("button", { name: /^Logs for / }).click();
  await expect.poll(() => sockets.length).toBe(2);
  expect(Object.fromEntries(new URL(sockets[1].url()).searchParams)).toEqual({
    dag_id: "next_dag",
    run_id: "next_run",
    task_id: "next_task",
    map_index: "-1",
    try_number: "1",
    tail: "1000",
  });
  await expect(page.getByTestId("log-text")).toHaveCount(0);
  await page.evaluate(() => window.airflowFixture.unmount());
  await expect.poll(() => closed).toBe(2);
});

test("a slow previous task cannot replace the selected task after host navigation", async ({
  page,
}) => {
  let release: () => void = () => {};
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  let started = false;
  await api(page, {
    capabilities: { ...capabilities, spark_history_url: "" },
    handle: async (route) => {
      const url = new URL(route.request().url());
      if (!url.pathname.endsWith("/application")) return false;
      const old = url.searchParams.get("task_id") === "aggregate";
      if (old) {
        started = true;
        await held;
      }
      await route
        .fulfill({
          json: application({
            app_id: old ? "old-app" : "new-app",
            name: old ? "Old task application" : "New task application",
          }),
        })
        .catch(() => {});
      return true;
    },
  });
  await page.goto("/tests/airflow-fixture.html");
  await expect.poll(() => started).toBe(true);
  await page.evaluate(() =>
    window.airflowFixture.render({
      dagId: "daily_sales",
      runId: "run-2",
      taskId: "next",
      mapIndex: -1,
      taskInstance: { try_number: 1 },
    }),
  );
  await expect(
    page.getByRole("button", { name: "New task application", exact: true }),
  ).toBeVisible();
  release();
  await expect(
    page.getByRole("button", { name: "Old task application", exact: true }),
  ).toHaveCount(0);
});

test("refresh applies revoked management permissions and application errors can be retried", async ({
  page,
}) => {
  let canManage = true;
  let missing = true;
  await api(page, {
    handle: async (route) => {
      const path = new URL(route.request().url()).pathname;
      if (path.endsWith("/capabilities")) {
        await route.fulfill({
          json: { ...capabilities, can_kill: canManage, can_delete: canManage },
        });
        return true;
      }
      if (path.endsWith("/application") && missing) {
        await route.fulfill({
          status: 404,
          json: {
            detail: {
              code: "driver_not_found",
              message: "The task driver is no longer available",
            },
          },
        });
        return true;
      }
      return false;
    },
  });
  await page.goto("/tests/airflow-fixture.html");
  await expect(page.getByRole("alert")).toContainText(
    "The task driver is no longer available",
  );
  missing = false;
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await expect(
    page
      .getByRole("dialog")
      .getByRole("button", { name: "Stop application", exact: true }),
  ).toBeVisible();
  await page.keyboard.press("Escape");
  canManage = false;
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Refresh", exact: true }),
  ).toBeEnabled();
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await expect(
    page
      .getByRole("dialog")
      .getByRole("button", { name: /Stop application|Delete application/ }),
  ).toHaveCount(0);
});

test("a removed driver preserves task namespace and history without exposing live actions", async ({
  page,
}) => {
  await api(page, {
    capabilities: { ...capabilities, can_kill: true, can_delete: true },
    handle: async (route) => {
      const path = new URL(route.request().url()).pathname;
      if (path.endsWith("/application")) {
        await route.fulfill({
          json: application({
            source: "history",
            status: "Unknown",
            namespace: "analytics",
            driver_pod: "report-1-driver",
            driver_logs: false,
            spark_ui_proxy: false,
            spark_history_proxy: true,
          }),
        });
        return true;
      }
      if (path.endsWith("/history")) {
        await route.fulfill({ json: { items: [] } });
        return true;
      }
      return false;
    },
  });
  await page.goto("/tests/airflow-fixture.html");
  await expect(
    page.getByText("Driver removed · task reference", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Archived · namespace unknown", { exact: true }),
  ).toHaveCount(0);
  await expect(page.getByRole("button", { name: /^Logs for / })).toHaveCount(0);
  await expect(page.getByRole("link", { name: /^Spark UI for / })).toHaveCount(
    0,
  );
  await expect(
    page.getByRole("link", { name: /^History for / }),
  ).toHaveAttribute("href", capabilities.spark_history_url);
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  const dialog = page.getByRole("dialog");
  await expect(
    dialog.getByText("Driver removed · task reference", { exact: true }),
  ).toBeVisible();
  await expect(
    dialog
      .locator(".detail-grid > div")
      .filter({ has: page.getByText("Namespace", { exact: true }) }),
  ).toContainText("analytics");
  await expect(
    dialog.getByRole("button", {
      name: /Stop application|Delete application|Logs/,
    }),
  ).toHaveCount(0);
  await expect(
    dialog.getByRole("link", { name: /^History for / }),
  ).toHaveAttribute("href", capabilities.spark_history_url);
});

test("native task details use the shared diagnostics view with task coordinates", async ({
  page,
}) => {
  const requests = await api(page, {
    handle: async (route) => {
      if (!new URL(route.request().url()).pathname.endsWith("/diagnostics"))
        return false;
      await route.fulfill({
        json: {
          diagnosis: {
            schema_version: 1,
            namespace: "analytics",
            app_id: "report-1",
            driver_pod: "report-1-driver",
            inspected_pods: 1,
            inspected_events: 1,
            incomplete: false,
            findings: [
              {
                code: "oom_killed",
                severity: "error",
                resource: "report-1-driver",
                container: "driver",
                summary: "Container was OOMKilled",
                evidence: "Observed OOMKilled",
                remediation: "Inspect memory usage.",
              },
            ],
          },
        },
      });
      return true;
    },
  });
  await page.goto(
    "/tests/airflow-fixture.html?base=/airflow/&dagId=dag&runId=run&taskId=task&mapIndex=2&tryNumber=1",
  );
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  await expect(
    page
      .getByRole("region", { name: "Application diagnostics" })
      .getByText("Container was OOMKilled"),
  ).toBeVisible();
  const request = requests.find((request) =>
    new URL(request.url()).pathname.endsWith("/diagnostics"),
  )!;
  expect(new URL(request.url()).pathname).toBe(
    "/airflow/spark-on-k8s/api/diagnostics",
  );
  expect(queryOf(request)).toEqual({
    dag_id: "dag",
    run_id: "run",
    task_id: "task",
    map_index: "2",
    try_number: "1",
  });
});
