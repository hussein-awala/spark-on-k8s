import { readFile } from "node:fs/promises";
import { expect, test, type WebSocketRoute } from "@playwright/test";
import { application } from "./data";

const diagnosis = {
  findings: [],
  inspected_pods: 1,
  inspected_events: 0,
  incomplete: false,
};

test("frozen reading survives live-buffer eviction and copy uses the snapshot", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  let socket: WebSocketRoute | undefined;
  await page.routeWebSocket("**/webserver/ws/logs/**", (route) => {
    socket = route;
  });
  await page.goto("/tests/fixture.html?view=logs&history=false");
  await expect.poll(() => socket !== undefined).toBe(true);
  socket!.send("ERROR original exception\nINFO original context");
  await expect(page.getByTestId("log-text")).toContainText(
    "original exception",
  );
  await page
    .getByRole("button", { name: "Freeze reading view", exact: true })
    .click();
  const resume = page.getByRole("button", {
    name: "Return to live output",
    exact: true,
  });
  await expect(resume).toBeFocused();
  socket!.send(
    Array.from({ length: 10050 }, (_, index) => `new-${index}`).join("\n"),
  );
  await expect(page.getByText(/10,050 new lines received/)).toContainText(
    "52 live-buffer lines discarded",
  );
  await expect(page.getByTestId("log-text")).toHaveText(
    "ERROR original exception\nINFO original context",
  );
  await expect(resume).toBeFocused();
  await page
    .getByRole("searchbox", { name: "Search logs", exact: true })
    .fill("ERROR");
  await page.getByRole("button", { name: "Copy visible", exact: true }).click();
  await expect
    .poll(() => page.evaluate(() => navigator.clipboard.readText()))
    .toBe("ERROR original exception");
  await page
    .getByRole("searchbox", { name: "Search logs", exact: true })
    .fill("");
  await resume.click();
  await expect(page.getByTestId("log-text")).toContainText("new-10049");
  const live = (await page.getByTestId("log-text").textContent())!.split("\n");
  expect(live).toHaveLength(10000);
  expect(live[0]).toBe("new-50");
});

test("prepared excerpts freeze visible selection, mask and exclude locally, and use a neutral filename", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  let socket: WebSocketRoute | undefined;
  await page.routeWebSocket("**/webserver/ws/logs/**", (route) => {
    socket = route;
  });
  await page.goto("/tests/fixture.html?view=logs&history=false");
  await expect.poll(() => socket !== undefined).toBe(true);
  socket!.send(
    Array.from({ length: 100 }, (_, i) =>
      i < 5 ? `ERROR ${i} customer-private token=sentinel-secret` : `INFO ${i}`,
    ).join("\n"),
  );
  await expect(page.getByTestId("log-text")).toContainText("INFO 99");
  await page
    .getByRole("searchbox", { name: "Search logs", exact: true })
    .fill("ERROR");
  const requests: string[] = [];
  page.on("request", (request) => requests.push(request.url()));
  await page
    .getByRole("button", { name: "Prepare excerpt", exact: true })
    .click();
  const preview = page.getByRole("textbox", {
    name: "Excerpt preview",
    exact: true,
  });
  expect((await preview.inputValue()).split("\n")).toHaveLength(5);
  socket!.send("ERROR arriving after preview opened");
  await expect(preview).not.toHaveValue(/arriving/);
  await preview.fill(
    (await preview.inputValue()).split("\n").slice(1).join("\n"),
  );
  await page
    .getByRole("textbox", { name: "Exact value to mask", exact: true })
    .fill("customer-private");
  await page
    .getByRole("button", { name: "Mask exact value", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Mask common credentials", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Copy prepared excerpt", exact: true })
    .click();
  const expected = [1, 2, 3, 4]
    .map((i) => `ERROR ${i} [MASKED] token=[MASKED]`)
    .join("\n");
  await expect
    .poll(() => page.evaluate(() => navigator.clipboard.readText()))
    .toBe(expected);
  const downloading = page.waitForEvent("download");
  await page
    .getByRole("button", { name: "Download prepared excerpt", exact: true })
    .click();
  const download = await downloading;
  expect(download.suggestedFilename()).toBe("spark-log-excerpt.txt");
  expect(await readFile((await download.path())!, "utf-8")).toBe(expected);
  expect(requests).toEqual([]);
});

test("excerpt previews enforce line and character bounds", async ({ page }) => {
  let socket: WebSocketRoute | undefined;
  await page.routeWebSocket("**/webserver/ws/logs/**", (route) => {
    socket = route;
  });
  await page.goto("/tests/fixture.html?view=logs&history=false");
  await expect.poll(() => socket !== undefined).toBe(true);
  socket!.send(Array.from({ length: 1005 }, (_, i) => `line-${i}`).join("\n"));
  await expect(page.getByTestId("log-text")).toContainText("line-1004");
  await page
    .getByRole("button", { name: "Prepare excerpt", exact: true })
    .click();
  const preview = page.getByRole("textbox", {
    name: "Excerpt preview",
    exact: true,
  });
  expect((await preview.inputValue()).split("\n")).toHaveLength(1000);
  await expect(page.getByText(/visible selection was truncated/)).toBeVisible();
  await preview.fill("x".repeat(100001));
  expect((await preview.inputValue()).length).toBe(100000);
});

test("details show fetch age and paused polling; explicit refresh preserves focus", async ({
  page,
}) => {
  await page.clock.install();
  let requests = 0;
  await page.route(/\/webserver\/api\/apps\?/, (route) => {
    requests += 1;
    return route.fulfill({
      json: { items: [application()], next_cursor: null },
    });
  });
  await page.route("**/diagnostics", (route) =>
    route.fulfill({ json: { diagnosis } }),
  );
  await page.goto("/tests/fixture.html?history=false&refresh=1");
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  const dialog = page.getByRole("dialog");
  await expect(
    dialog.getByText(/Auto-refresh paused while details are open/),
  ).toBeVisible();
  const before = requests;
  await page.clock.fastForward(5000);
  await expect(
    dialog.getByText(/Application snapshot fetched 5s ago/),
  ).toBeVisible();
  expect(requests).toBe(before);
  const refresh = dialog.getByRole("button", {
    name: "Refresh application snapshot",
    exact: true,
  });
  await refresh.click();
  await expect.poll(() => requests).toBe(before + 1);
  await expect(refresh).toBeFocused();
  await expect(
    dialog.getByText(/Application snapshot fetched 0s ago/),
  ).toBeVisible();
});

test("details of an application that leaves the list close and resume polling", async ({
  page,
}) => {
  await page.clock.install();
  let requests = 0;
  let listed = true;
  await page.route(/\/webserver\/api\/apps\?/, (route) => {
    requests += 1;
    return route.fulfill({
      json: { items: listed ? [application()] : [], next_cursor: null },
    });
  });
  await page.route("**/diagnostics", (route) =>
    route.fulfill({ json: { diagnosis } }),
  );
  await page.goto("/tests/fixture.html?history=false&refresh=1");
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  const dialog = page.getByRole("dialog");
  await expect(dialog).toBeVisible();
  listed = false;
  await dialog
    .getByRole("button", { name: "Refresh application snapshot" })
    .click();
  await expect(dialog).toBeHidden();
  await expect(page.getByRole("status")).toContainText(
    "The selected application is no longer listed",
  );
  await expect(
    page.getByRole("heading", { level: 1, name: /Applications/ }),
  ).toBeFocused();
  const before = requests;
  await page.clock.fastForward(2500);
  await expect.poll(() => requests).toBeGreaterThan(before);
  // A later listing of the same application must not reopen stale details.
  listed = true;
  await page.clock.fastForward(1500);
  await expect(
    page.getByRole("button", { name: "Daily revenue report", exact: true }),
  ).toBeVisible();
  await expect(dialog).toBeHidden();
});

for (const origin of ["row", "dialog"] as const) {
  for (const removed of [false, true]) {
    test(`embedded log navigation restores keyboard orientation from ${origin}; removed=${removed}`, async ({
      page,
    }) => {
      let items = [application()];
      await page.route(/\/webserver\/api\/apps\?/, (route) =>
        route.fulfill({ json: { items, next_cursor: null } }),
      );
      await page.route("**/diagnostics", (route) =>
        route.fulfill({ json: { diagnosis } }),
      );
      await page.routeWebSocket("**/webserver/ws/logs/**", () => {});
      await page.goto("/tests/fixture.html?history=false&embedded=true");
      if (origin === "dialog")
        await page
          .getByRole("button", { name: "Daily revenue report", exact: true })
          .click();
      const scope =
        origin === "dialog"
          ? page.getByRole("dialog")
          : page.getByRole("table");
      await scope
        .getByRole("button", {
          name: "Logs for Daily revenue report (report-1)",
          exact: true,
        })
        .focus();
      await page.keyboard.press("Enter");
      await expect(
        page.getByRole("heading", { name: "Application logs." }),
      ).toBeFocused();
      if (removed) items = [];
      await page
        .getByRole("button", { name: "Applications", exact: true })
        .focus();
      await page.keyboard.press("Enter");
      if (removed) {
        await expect(
          page.getByRole("heading", { name: "Applications." }),
        ).toBeFocused();
        await expect(
          page.getByText(/previous application's Logs control is unavailable/),
        ).toBeVisible();
      } else {
        await expect(
          page.getByRole("button", {
            name: "Logs for Daily revenue report (report-1)",
            exact: true,
          }),
        ).toBeFocused();
      }
    });
  }
}

test("quick links distinguish identically named workloads and confirmation includes exact identity", async ({
  page,
}) => {
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({
      json: {
        items: [
          application(),
          application({ app_id: "report-2", driver_pod: "report-2-driver" }),
        ],
        next_cursor: null,
      },
    }),
  );
  await page.goto("/tests/fixture.html?history=false");
  await expect(
    page.getByRole("link", {
      name: "Logs for Daily revenue report (report-1)",
      exact: true,
    }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", {
      name: "Logs for Daily revenue report (report-2)",
      exact: true,
    }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .nth(1)
    .click();
  await page
    .getByRole("button", { name: "Delete application", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toContainText(
    "Application ID: report-2 · Driver pod: report-2-driver",
  );
});

test("a successful diagnostic snapshot can refresh and stays visible after refresh failure", async ({
  page,
}) => {
  let fail = false;
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({ json: { items: [application()], next_cursor: null } }),
  );
  await page.route("**/diagnostics", (route) =>
    route.fulfill(
      fail
        ? { status: 503, json: { detail: { message: "Temporary failure" } } }
        : { json: { diagnosis } },
    ),
  );
  await page.goto("/tests/fixture.html?history=false");
  await page
    .getByRole("button", { name: "Daily revenue report", exact: true })
    .click();
  const section = page.getByRole("region", { name: "Application diagnostics" });
  await expect(section.getByText(/Diagnostic snapshot fetched/)).toBeVisible();
  fail = true;
  await section
    .getByRole("button", { name: "Refresh diagnostics", exact: true })
    .click();
  await expect(
    section.getByText("Previous diagnostic snapshot is still shown."),
  ).toBeVisible();
  await expect(
    section.getByText("No specific problems observed in this snapshot."),
  ).toBeVisible();
});
