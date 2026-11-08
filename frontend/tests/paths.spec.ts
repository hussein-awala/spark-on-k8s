import { expect, test, type WebSocketRoute } from "@playwright/test";
import { application } from "./data";

const invalidPrefixes = [
  "javascript:alert(1)//",
  "data:text/html,evil",
  "https://evil.example",
  "//evil.example",
  "/\\evil.example",
  "\t//evil.example",
  "/\t/evil.example",
  "/prefix/..//evil.example",
  "/prefix/%2e%2e//evil.example",
  "/prefix/.%2E//evil.example",
  "/%2f%2fevil.example",
  "/%5cevil.example",
  "/bad%0aprefix",
  "/bad%zz",
  "/bad%ff",
  "/prefix?override=1",
  "/prefix#anchor",
];

for (const basePath of invalidPrefixes) {
  test(`invalid deployment prefix stays inert: ${JSON.stringify(basePath)}`, async ({
    page,
    baseURL,
  }) => {
    const unexpected: string[] = [];
    const sockets: string[] = [];
    await page.route("**/*", (route) => {
      const url = new URL(route.request().url());
      if (
        url.origin !== new URL(baseURL!).origin ||
        url.pathname.includes("/webserver/") ||
        url.pathname.endsWith("/runs/v1")
      ) {
        unexpected.push(url.href);
        return route.abort();
      }
      return route.continue();
    });
    await page.routeWebSocket("**/webserver/ws/logs/**", (socket) => {
      sockets.push(socket.url());
    });
    const params = new URLSearchParams({
      basePath,
      view: "logs",
      runs: "true",
    });
    for (const view of ["apps", "runs", "logs"]) {
      params.set("view", view);
      await page.goto(`/tests/fixture.html?${params}`);
      await expect(page.getByRole("alert")).toContainText(
        "The console base path is invalid",
      );
      await expect(page.getByRole("link")).toHaveCount(0);
      expect(unexpected).toEqual([]);
      expect(sockets).toEqual([]);
      expect(new URL(page.url()).origin).toBe(new URL(baseURL!).origin);
    }
  });
}

for (const [basePath, prefix] of [
  ["", ""],
  ["/", ""],
  ["/nested/spark", "/nested/spark"],
  ["/nested/spark/", "/nested/spark"],
  ["/nested/spark///", "/nested/spark"],
  ["/team%20one/équipe", "/team%20one/%C3%A9quipe"],
]) {
  test(`console links and transports preserve mount ${JSON.stringify(basePath)}`, async ({
    page,
    baseURL,
  }) => {
    const namespace = "team & #/one";
    const appId = "report/one?part#first";
    const requests: URL[] = [];
    const sockets: WebSocketRoute[] = [];
    await page.route(/\/webserver\/api\/apps\?/, (route) => {
      requests.push(new URL(route.request().url()));
      return route.fulfill({
        json: {
          items: [application({ namespace, app_id: appId })],
          next_cursor: null,
        },
      });
    });
    await page.route(/\/runs\/v1\?/, (route) => {
      requests.push(new URL(route.request().url()));
      return route.fulfill({
        json: { schema_version: 1, items: [], next_cursor: null },
      });
    });
    await page.routeWebSocket("**/webserver/ws/logs/**", (socket) => {
      sockets.push(socket);
    });
    const params = new URLSearchParams({
      basePath,
      namespace,
      appId,
      history: "false",
      runs: "true",
    });
    await page.goto(`/tests/fixture.html?${params}`);
    await expect(
      page.getByRole("button", { name: "Daily revenue report", exact: true }),
    ).toBeVisible();
    for (const [name, suffix] of [
      [
        "Applications",
        `/webserver/apps?namespace=${encodeURIComponent(namespace)}`,
      ],
      [
        "Recorded runs",
        `/webserver/runs?namespace=${encodeURIComponent(namespace)}`,
      ],
      [
        "Logs",
        `/webserver/logs/${encodeURIComponent(namespace)}/${encodeURIComponent(appId)}?tail=1000`,
      ],
      [
        "Spark UI",
        `/webserver/ui/${encodeURIComponent(namespace)}/${encodeURIComponent(appId)}`,
      ],
    ]) {
      const link = page.getByRole("link", {
        name: ["Logs", "Spark UI"].includes(name)
          ? new RegExp(`^${name} for `)
          : name,
        exact: true,
      });
      await expect(link).toHaveAttribute("href", `${prefix}${suffix}`);
      expect(
        await link.evaluate((element: HTMLAnchorElement) => element.origin),
      ).toBe(new URL(baseURL!).origin);
    }
    expect(requests[0].pathname).toBe(`${prefix}/webserver/api/apps`);
    expect(requests[0].searchParams.get("namespace")).toBe(namespace);
    params.set("view", "runs");
    await page.goto(`/tests/fixture.html?${params}`);
    await expect(
      page.getByRole("heading", { name: "No recorded runs" }),
    ).toBeVisible();
    expect(requests.at(-1)!.pathname).toBe(`${prefix}/runs/v1`);
    expect(requests.at(-1)!.searchParams.get("namespace")).toBe(namespace);
    expect(
      requests.every((url) => url.origin === new URL(baseURL!).origin),
    ).toBe(true);
    params.set("view", "logs");
    await page.goto(`/tests/fixture.html?${params}`);
    await expect.poll(() => sockets.length).toBe(1);
    const socket = new URL(sockets[0].url());
    expect(socket.host).toBe(new URL(baseURL!).host);
    expect(socket.pathname).toBe(
      `${prefix}/webserver/ws/logs/${encodeURIComponent(namespace)}/${encodeURIComponent(appId)}`,
    );
    expect(socket.search).toBe("?tail=1000");
  });
}

test("valid-invalid-valid configuration changes close logs and remount without hook errors", async ({
  page,
}) => {
  const errors: string[] = [];
  const sockets: WebSocketRoute[] = [];
  let closed = 0;
  page.on("pageerror", (error) => errors.push(error.message));
  await page.routeWebSocket("**/webserver/ws/logs/**", (socket) => {
    sockets.push(socket);
    socket.onClose(() => {
      closed += 1;
    });
  });
  await page.goto("/tests/fixture.html?view=logs&history=false");
  await expect.poll(() => sockets.length).toBe(1);
  await page.evaluate(() => {
    (
      window as unknown as {
        updateConsoleConfig: (value: { basePath: string }) => void;
      }
    ).updateConsoleConfig({ basePath: "//evil.example" });
  });
  await expect(page.getByRole("alert")).toContainText(
    "The console base path is invalid",
  );
  await expect.poll(() => closed).toBe(1);
  expect(sockets).toHaveLength(1);
  await page.evaluate(() => {
    (
      window as unknown as {
        updateConsoleConfig: (value: { basePath: string }) => void;
      }
    ).updateConsoleConfig({ basePath: "/nested/spark" });
  });
  await expect(page.getByRole("alert")).toHaveCount(0);
  await expect.poll(() => sockets.length).toBe(2);
  sockets[1].send("Reconnected safely");
  await expect(page.getByTestId("log-text")).toHaveText("Reconnected safely");
  expect(errors).toEqual([]);
});

test("large slash runs are handled without backtracking", async ({ page }) => {
  test.setTimeout(15_000);
  await page.routeWebSocket("**/webserver/ws/logs/**", () => {});
  await page.goto("/tests/fixture.html?view=logs&runs=true&history=false");
  const recordedRuns = page.getByRole("link", {
    name: "Recorded runs",
    exact: true,
  });
  await expect(recordedRuns).toBeVisible();
  await page.evaluate(() => {
    (
      window as unknown as {
        updateConsoleConfig: (value: {
          basePath: string;
          namespace: string;
        }) => void;
      }
    ).updateConsoleConfig({
      basePath: "/large/mount" + "/".repeat(100_000),
      namespace: "large-prefix",
    });
  });
  await expect(recordedRuns).toHaveAttribute(
    "href",
    "/large/mount/webserver/runs?namespace=large-prefix",
  );
  await page.evaluate(() => {
    (
      window as unknown as {
        updateConsoleConfig: (value: { basePath: string }) => void;
      }
    ).updateConsoleConfig({
      basePath: "/nested/" + "/".repeat(100_000) + "suffix",
    });
  });
  await expect(page.getByRole("alert")).toContainText(
    "The console base path is invalid",
  );
});
