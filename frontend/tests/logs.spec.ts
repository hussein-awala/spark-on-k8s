import { readFile } from "node:fs/promises";
import { expect, test, type WebSocketRoute } from "@playwright/test";

test("logs remain literal, searchable, copyable, downloadable, and clearable", async ({
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
  const attack = '<img src=x onerror="window.logExecuted=true">';
  socket!.send(attack);
  socket!.send("INFO startup complete\nERROR failed < threshold && count > 0");
  await expect(page.getByTestId("log-text")).toContainText(attack);
  await expect(
    page.locator(".log-viewport img, .log-viewport script"),
  ).toHaveCount(0);
  expect(
    await page.evaluate(
      () => (window as unknown as { logExecuted?: boolean }).logExecuted,
    ),
  ).toBeUndefined();
  await page
    .getByRole("searchbox", { name: "Search logs", exact: true })
    .fill("ERROR failed");
  await expect(page.getByTestId("log-text")).toHaveText(
    "ERROR failed < threshold && count > 0",
  );
  await page.getByRole("button", { name: "Copy visible", exact: true }).click();
  await expect
    .poll(() => page.evaluate(() => navigator.clipboard.readText()))
    .toBe("ERROR failed < threshold && count > 0");
  const downloading = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download", exact: true }).click();
  const download = await downloading;
  expect(download.suggestedFilename()).toBe("report-1.log");
  expect(await readFile((await download.path())!, "utf-8")).toBe(
    `${attack}\nINFO startup complete\nERROR failed < threshold && count > 0`,
  );
  await page.getByRole("button", { name: "Clear", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Copy visible", exact: true }),
  ).toBeDisabled();
  await page
    .getByRole("searchbox", { name: "Search logs", exact: true })
    .fill("");
  socket!.send("new output after clearing");
  await expect(page.getByTestId("log-text")).toHaveText(
    "new output after clearing",
  );
});

test("log buffering is bounded, paused autoscroll still receives logs, and wrap hides line numbers", async ({
  page,
}) => {
  let socket: WebSocketRoute | undefined;
  await page.routeWebSocket("**/webserver/ws/logs/**", (route) => {
    socket = route;
  });
  await page.goto("/tests/fixture.html?view=logs&history=false");
  await expect.poll(() => socket !== undefined).toBe(true);
  socket!.send(
    Array.from({ length: 10050 }, (_, index) => `line-${index}`).join("\n"),
  );
  await expect(page.getByTestId("log-text")).toContainText("line-10049");
  const retained = (await page.getByTestId("log-text").textContent())!.split(
    "\n",
  );
  expect(retained).toHaveLength(10000);
  expect(retained[0]).toBe("line-50");
  expect(
    (await page.locator(".log-numbers").textContent())!.split("\n")[0],
  ).toBe("51");
  await expect(page.locator(".log-footer")).toContainText(
    "50 older lines discarded",
  );
  await page
    .getByRole("button", { name: "Pause auto-scroll", exact: true })
    .click();
  const previous = await page
    .getByLabel("Log output", { exact: true })
    .evaluate((element) => element.scrollTop);
  socket!.send("arrived while paused");
  await expect(page.getByTestId("log-text")).toContainText(
    "arrived while paused",
  );
  await expect(page.locator(".log-footer")).toContainText("Auto-scroll paused");
  expect(
    await page
      .getByLabel("Log output", { exact: true })
      .evaluate((element) => element.scrollTop),
  ).toBe(previous);
  await page.getByRole("checkbox", { name: "Wrap lines", exact: true }).check();
  await expect(page.locator(".log-numbers")).toBeHidden();
  await page
    .getByRole("button", { name: "Resume auto-scroll", exact: true })
    .click();
  await expect(page.locator(".log-footer")).toContainText("Following output");
});

test("tail changes and reconnects open fresh streams and report policy failures", async ({
  page,
}) => {
  const sockets: WebSocketRoute[] = [];
  await page.routeWebSocket("**/webserver/ws/logs/**", (route) => {
    sockets.push(route);
  });
  await page.goto(
    "/tests/fixture.html?view=logs&history=false&appId=app%2Fone",
  );
  await expect.poll(() => sockets.length).toBe(1);
  expect(sockets[0].url()).toContain(
    "/nested/spark/webserver/ws/logs/analytics/app%2Fone?tail=1000",
  );
  sockets[0].send("old stream output");
  await expect(page.getByTestId("log-text")).toHaveText("old stream output");
  await page
    .getByRole("combobox", { name: "Initial lines", exact: true })
    .selectOption("100");
  await expect.poll(() => sockets.length).toBe(2);
  expect(sockets[1].url()).toContain("?tail=100");
  await expect(page.getByTestId("log-text")).toHaveCount(0);
  await sockets[1].close({ code: 1008, reason: "Not available" });
  await expect(page.getByRole("alert")).toContainText("Logs are unavailable");
  await page.getByRole("button", { name: "Reconnect", exact: true }).click();
  await expect.poll(() => sockets.length).toBe(3);
  sockets[2].send("new stream output");
  await expect(page.getByTestId("log-text")).toHaveText("new stream output");
  await sockets[2].close({ code: 1000 });
  await expect(page.getByText("Stream ended", { exact: true })).toBeVisible();
  await expect(page.getByTestId("log-text")).toHaveText("new stream output");
});

test("leaving embedded logs closes the stream and reconnecting cannot reuse stale output", async ({
  page,
}) => {
  let closed = 0;
  const sockets: WebSocketRoute[] = [];
  await page.route(/\/webserver\/api\/apps\?/, (route) =>
    route.fulfill({ json: { items: [], next_cursor: null } }),
  );
  await page.routeWebSocket("**/webserver/ws/logs/**", (route) => {
    sockets.push(route);
    route.onClose(() => {
      closed += 1;
    });
  });
  await page.goto("/tests/fixture.html?view=logs&history=false&embedded=true");
  await expect.poll(() => sockets.length).toBe(1);
  sockets[0].send("old connection");
  await expect(page.getByTestId("log-text")).toHaveText("old connection");
  await page.getByRole("button", { name: "Reconnect", exact: true }).click();
  await expect.poll(() => sockets.length).toBe(2);
  await expect.poll(() => closed).toBe(1);
  await expect(page.getByTestId("log-text")).toHaveCount(0);
  sockets[1].send("new connection");
  await expect(page.getByTestId("log-text")).toHaveText("new connection");
  await page.getByRole("button", { name: "Applications", exact: true }).click();
  await expect.poll(() => closed).toBe(2);
  await expect(
    page.getByRole("heading", { name: "Applications." }),
  ).toBeVisible();
});
