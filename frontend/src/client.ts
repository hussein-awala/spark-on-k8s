import type {
  ApplicationPage,
  ConsoleApplication,
  ConsoleConfig,
  ConsoleTransport,
  Diagnosis,
  RunPage,
} from "./types";

function pathSegments(path: string, prefix: boolean): string[] {
  if (
    !path.startsWith("/") ||
    path.startsWith("//") ||
    /[\\\u0000-\u001f\u007f?#]/u.test(path)
  )
    throw new Error("Invalid console path");
  let end = path.length;
  while (prefix && end > 0 && path[end - 1] === "/") end -= 1;
  const pathname = path.slice(0, end);
  if (!pathname || pathname === "/") return [];
  return pathname
    .slice(1)
    .split("/")
    .map((segment) => {
      const decoded = decodeURIComponent(segment);
      if (
        (prefix && !segment) ||
        decoded === "." ||
        decoded === ".." ||
        /[\u0000-\u001f\u007f]/u.test(decoded) ||
        (prefix && /[/\\]/u.test(decoded))
      )
        throw new Error("Invalid console path");
      return encodeURIComponent(decoded);
    });
}

export function consolePath(basePath: string, path: string): string {
  const queryStart = path.indexOf("?");
  const pathname = queryStart < 0 ? path : path.slice(0, queryStart);
  const query = queryStart < 0 ? "" : path.slice(queryStart);
  if (/[#\u0000-\u001f\u007f]/u.test(query))
    throw new Error("Invalid console query");
  const prefix = basePath ? pathSegments(basePath, true) : [];
  const route = pathSegments(pathname, false);
  // Encode each component before adding separators: a normalized prefix must
  // never become a scheme or a network-path reference such as //other-host.
  const trailingSlash = pathname === "/" && prefix.length ? "/" : "";
  return `/${[...prefix, ...route].join("/")}${trailingSlash}${query}`;
}

export function appLogsPath(
  config: ConsoleConfig,
  app: ConsoleApplication,
): string {
  return consolePath(
    config.basePath,
    `/webserver/logs/${encodeURIComponent(app.namespace ?? "")}/${encodeURIComponent(app.app_id)}?tail=1000`,
  );
}

export function appUIPath(
  config: ConsoleConfig,
  app: ConsoleApplication,
): string {
  return consolePath(
    config.basePath,
    `/webserver/ui/${encodeURIComponent(app.namespace ?? "")}/${encodeURIComponent(app.app_id)}`,
  );
}

export function appHistoryPath(
  config: ConsoleConfig,
  app: ConsoleApplication,
): string {
  return consolePath(
    config.basePath,
    `/webserver/ui-history/history/${encodeURIComponent(app.app_id)}`,
  );
}

export function errorMessage(error: unknown): string {
  return error instanceof Error
    ? error.message
    : "Something went wrong. Please try again.";
}

export async function checkResponse(response: Response): Promise<Response> {
  if (response.ok) return response;
  let message = `The request failed (${response.status}). Please try again.`;
  const body = await response.text();
  try {
    const data = JSON.parse(body) as {
      detail?: string | { message?: string };
      message?: string;
      error?: { message?: string };
    };
    const detail =
      typeof data.detail === "string" ? data.detail : data.detail?.message;
    message = detail || data.message || data.error?.message || message;
  } catch {
    if (body && !body.trimStart().startsWith("<")) message = body.slice(0, 500);
  }
  throw new Error(message);
}

export function createConsoleTransport(
  config: Pick<ConsoleConfig, "basePath">,
): ConsoleTransport {
  const url = (path: string) => consolePath(config.basePath, path);
  return {
    async listRuns({
      namespace,
      status,
      presence,
      limit,
      cursor,
      signal,
    }): Promise<RunPage> {
      const query = new URLSearchParams({ namespace, limit: String(limit) });
      if (status) query.set("status", status);
      if (presence) query.set("presence", presence);
      if (cursor) query.set("cursor", cursor);
      const response = await checkResponse(
        await fetch(url(`/runs/v1?${query}`), {
          signal,
          credentials: "same-origin",
        }),
      );
      return response.json() as Promise<RunPage>;
    },
    async diagnoseApp(app, signal): Promise<Diagnosis> {
      const response = await checkResponse(
        await fetch(
          url(
            `/webserver/api/apps/${encodeURIComponent(app.namespace ?? "")}/pods/${encodeURIComponent(app.driver_pod ?? "")}/diagnostics`,
          ),
          { signal, credentials: "same-origin" },
        ),
      );
      return ((await response.json()) as { diagnosis: Diagnosis }).diagnosis;
    },
    async listApps({
      namespace,
      limit,
      cursor,
      signal,
    }): Promise<ApplicationPage> {
      const query = new URLSearchParams({ namespace, limit: String(limit) });
      if (cursor) query.set("continue_from", cursor);
      const response = await checkResponse(
        await fetch(url(`/webserver/api/apps?${query}`), {
          signal,
          credentials: "same-origin",
        }),
      );
      return response.json() as Promise<ApplicationPage>;
    },
    async listHistory({ signal }): Promise<ConsoleApplication[]> {
      const response = await checkResponse(
        await fetch(url("/webserver/api/history"), {
          signal,
          credentials: "same-origin",
        }),
      );
      const data = (await response.json()) as { items: ConsoleApplication[] };
      return data.items;
    },
    async killApp(namespace, appId): Promise<void> {
      await checkResponse(
        await fetch(
          url(
            `/app/${encodeURIComponent(namespace)}/${encodeURIComponent(appId)}`,
          ),
          { method: "PATCH", credentials: "same-origin" },
        ),
      );
    },
    async deleteApp(namespace, appId): Promise<void> {
      await checkResponse(
        await fetch(
          url(
            `/app/${encodeURIComponent(namespace)}/${encodeURIComponent(appId)}`,
          ),
          { method: "DELETE", credentials: "same-origin" },
        ),
      );
    },
    openLogs(namespace, appId, tail): WebSocket {
      const target = new URL(
        url(
          `/webserver/ws/logs/${encodeURIComponent(namespace)}/${encodeURIComponent(appId)}?tail=${tail}`,
        ),
        window.location.origin,
      );
      target.protocol = target.protocol === "https:" ? "wss:" : "ws:";
      return new WebSocket(target);
    },
  };
}

export function mergeApplications(
  live: ConsoleApplication[],
  history: ConsoleApplication[],
): ConsoleApplication[] {
  const applications = new Map(live.map((app) => [app.app_id, app]));
  for (const archive of history) {
    const existing = applications.get(archive.app_id);
    applications.set(
      archive.app_id,
      existing
        ? { ...existing, spark_history_proxy: true }
        : {
            ...archive,
            source: "history",
            namespace: null,
            driver_logs: false,
            spark_ui_proxy: false,
          },
    );
  }
  return [...applications.values()];
}

export function canManage(app: ConsoleApplication): boolean {
  return (
    app.source === "kubernetes" && app.driver_logs && Boolean(app.namespace)
  );
}
