import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  appHistoryPath,
  appLogsPath,
  appUIPath,
  canManage,
  errorMessage,
  mergeApplications,
} from "./client";
import { Diagnostics } from "./Diagnostics";
import { Dialog } from "./Dialog";
import { Icon } from "./icons";
import type {
  ConsoleApplication,
  ConsoleConfig,
  ConsoleTransport,
} from "./types";

const PAGE_SIZE = 50;
const statuses = ["Running", "Pending", "Succeeded", "Failed", "Unknown"];

export function StatusBadge({ status }: { status: string }) {
  const known = statuses.includes(status) ? status.toLowerCase() : "unknown";
  return (
    <span className={`status-badge status-${known}`}>
      <span className="status-dot" />
      {status || "Unknown"}
    </span>
  );
}

function dateLabel(value: string | null): string {
  if (!value) return "—";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime())
    ? "—"
    : parsed.toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
}

function AppLinks({
  app,
  config,
  transport,
  onOpenLogs,
}: {
  app: ConsoleApplication;
  config: ConsoleConfig;
  transport: ConsoleTransport;
  onOpenLogs: (app: ConsoleApplication) => void;
}) {
  const uiUrl = transport.sparkUiUrl
    ? transport.sparkUiUrl(app)
    : appUIPath(config, app);
  const historyUrl = transport.historyUrl
    ? transport.historyUrl(app)
    : appHistoryPath(config, app);
  return (
    <div className="app-links">
      {app.driver_logs &&
        app.namespace &&
        (config.embedded ? (
          <button className="text-link" onClick={() => onOpenLogs(app)}>
            <Icon name="logs" />
            Logs
          </button>
        ) : (
          <a className="text-link" href={appLogsPath(config, app)}>
            <Icon name="logs" />
            Logs
          </a>
        ))}
      {app.spark_ui_proxy &&
        app.namespace &&
        app.status === "Running" &&
        uiUrl && (
          <a
            className="text-link"
            href={uiUrl}
            target="_blank"
            rel="noopener noreferrer"
          >
            Spark UI
            <Icon name="external" />
          </a>
        )}
      {app.spark_history_proxy && historyUrl && (
        <a
          className="text-link"
          href={historyUrl}
          target="_blank"
          rel="noopener noreferrer"
        >
          History
          <Icon name="external" />
        </a>
      )}
      {!app.driver_logs && !app.spark_ui_proxy && !app.spark_history_proxy && (
        <span className="muted">No links available</span>
      )}
    </div>
  );
}

interface ApplicationsProps {
  config: ConsoleConfig;
  transport: ConsoleTransport;
  namespaceEditable: boolean;
  onOpenLogs: (app: ConsoleApplication) => void;
  onNamespaceChange: (namespace: string) => void;
}

export function Applications({
  config,
  transport,
  namespaceEditable,
  onOpenLogs,
  onNamespaceChange,
}: ApplicationsProps) {
  const namespace = config.namespace;
  const [namespaceInput, setNamespaceInput] = useState(config.namespace);
  const [live, setLive] = useState<ConsoleApplication[]>([]);
  const [history, setHistory] = useState<ConsoleApplication[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [initialLoading, setInitialLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(config.historyEnabled);
  const [error, setError] = useState<string | null>(null);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [refreshInterval, setRefreshInterval] = useState(
    Math.max(0, config.refreshInterval),
  );
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [confirmation, setConfirmation] = useState<{
    app: ConsoleApplication;
    action: "kill" | "delete";
  } | null>(null);
  const [actionBusy, setActionBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [feedback, setFeedback] = useState<string | null>(null);
  const appRequest = useRef<AbortController | null>(null);
  const historyRequest = useRef<AbortController | null>(null);
  const loadedCount = useRef(0);
  const busy = useRef(false);

  const loadApps = useCallback(
    async (append = false, pageCursor?: string) => {
      appRequest.current?.abort();
      const controller = new AbortController();
      appRequest.current = controller;
      busy.current = true;
      if (append) setLoadingMore(true);
      else setRefreshing(true);
      setError(null);
      try {
        const items: ConsoleApplication[] = [];
        let next = pageCursor;
        const targetCount = append
          ? PAGE_SIZE
          : Math.max(PAGE_SIZE, loadedCount.current);
        const visited = new Set<string>();
        let nextCursor: string | null = null;
        do {
          const page = await transport.listApps({
            namespace,
            limit: PAGE_SIZE,
            cursor: next,
            signal: controller.signal,
          });
          items.push(...page.items);
          nextCursor = page.next_cursor;
          if (append || !nextCursor || items.length >= targetCount) break;
          if (visited.has(nextCursor))
            throw new Error(
              "The server returned a repeated page. Please refresh and try again.",
            );
          visited.add(nextCursor);
          next = nextCursor;
        } while (!controller.signal.aborted);
        if (controller.signal.aborted) return;
        setLive((previous) => {
          const result = [
            ...new Map(
              (append ? [...previous, ...items] : items).map((app) => [
                app.app_id,
                app,
              ]),
            ).values(),
          ];
          loadedCount.current = result.length;
          return result;
        });
        setCursor(nextCursor);
        setUpdatedAt(new Date());
      } catch (failure) {
        if (!controller.signal.aborted) setError(errorMessage(failure));
      } finally {
        if (appRequest.current === controller && !controller.signal.aborted) {
          busy.current = false;
          setInitialLoading(false);
          setRefreshing(false);
          setLoadingMore(false);
        }
      }
    },
    [namespace, transport],
  );

  const loadHistory = useCallback(async () => {
    historyRequest.current?.abort();
    if (!config.historyEnabled) {
      setHistory([]);
      setHistoryLoading(false);
      return;
    }
    const controller = new AbortController();
    historyRequest.current = controller;
    setHistoryLoading(true);
    setHistoryError(null);
    try {
      const items = await transport.listHistory({ signal: controller.signal });
      if (!controller.signal.aborted) setHistory(items);
    } catch (failure) {
      if (!controller.signal.aborted) setHistoryError(errorMessage(failure));
    } finally {
      if (!controller.signal.aborted) setHistoryLoading(false);
    }
  }, [config.historyEnabled, transport]);

  useEffect(() => {
    setNamespaceInput(config.namespace);
  }, [config.namespace]);

  useEffect(() => {
    loadedCount.current = 0;
    setLive([]);
    setCursor(null);
    setSelectedId(null);
    setUpdatedAt(null);
    setInitialLoading(true);
    void loadApps();
    return () => appRequest.current?.abort();
  }, [loadApps]);

  useEffect(() => {
    void loadHistory();
    return () => historyRequest.current?.abort();
  }, [loadHistory]);

  useEffect(() => {
    if (!refreshInterval || selectedId || confirmation) return;
    const timer = window.setInterval(() => {
      if (!document.hidden && !busy.current) {
        void loadApps();
        void loadHistory();
      }
    }, refreshInterval * 1000);
    return () => window.clearInterval(timer);
  }, [refreshInterval, loadApps, loadHistory, selectedId, confirmation]);

  const apps = useMemo(() => mergeApplications(live, history), [live, history]);
  const filtered = useMemo(() => {
    const search = query.trim().toLowerCase();
    return apps.filter(
      (app) =>
        (statusFilter === "all" || app.status === statusFilter) &&
        (!search ||
          [app.app_id, app.name, app.driver_pod, app.image].some((value) =>
            value?.toLowerCase().includes(search),
          )),
    );
  }, [apps, query, statusFilter]);
  const selected = apps.find((app) => app.app_id === selectedId);
  const running = apps.filter((app) => app.status === "Running").length;
  const pending = apps.filter((app) => app.status === "Pending").length;
  const failed = apps.filter((app) => app.status === "Failed").length;
  const intervals = [...new Set([0, 5, 10, 30, 60, refreshInterval])].sort(
    (a, b) => a - b,
  );

  async function performAction() {
    if (!confirmation?.app.namespace || !canManage(confirmation.app)) return;
    setActionBusy(true);
    setActionError(null);
    try {
      const { app, action } = confirmation;
      if (action === "kill")
        await transport.killApp(app.namespace!, app.app_id);
      else await transport.deleteApp(app.namespace!, app.app_id);
      setFeedback(
        action === "kill"
          ? `Stop requested for ${app.name || app.app_id}.`
          : `${app.name || app.app_id} was deleted.`,
      );
      setConfirmation(null);
      if (action === "delete") setSelectedId(null);
      void loadApps();
    } catch (failure) {
      setActionError(errorMessage(failure));
    } finally {
      setActionBusy(false);
    }
  }

  function confirm(app: ConsoleApplication, action: "kill" | "delete") {
    setActionError(null);
    setConfirmation({ app, action });
  }

  return (
    <>
      <header className="page-heading">
        <div>
          <div className="eyebrow">
            {config.contextLabel || "SPARK ON KUBERNETES"}
          </div>
          <h1>
            Applications<span className="heading-dot">.</span>
          </h1>
          <p className="page-description">
            Inspect application status, open Spark UIs, and manage driver logs.
          </p>
        </div>
        <button
          type="button"
          className="button button-secondary refresh-button"
          disabled={refreshing || loadingMore}
          onClick={() => {
            void loadApps();
            void loadHistory();
          }}
        >
          <Icon name="refresh" className={refreshing ? "spinning" : ""} />
          {refreshing && !initialLoading ? "Refreshing…" : "Refresh"}
        </button>
      </header>

      <div className="scope-bar">
        {namespaceEditable ? (
          <form
            className="namespace-form"
            onSubmit={(event) => {
              event.preventDefault();
              if (namespaceInput.trim())
                onNamespaceChange(namespaceInput.trim());
            }}
          >
            <label htmlFor="console-namespace">
              <Icon name="layers" />
              Namespace
            </label>
            <input
              id="console-namespace"
              value={namespaceInput}
              onChange={(event) => setNamespaceInput(event.target.value)}
              required
              maxLength={63}
              pattern="[a-z0-9](([a-z0-9]|-)*[a-z0-9])?"
              title="Use lowercase letters, numbers, and hyphens; start and end with a letter or number."
              autoComplete="off"
              spellCheck={false}
            />
            <button
              className="button button-small button-secondary"
              type="submit"
              disabled={
                !namespaceInput.trim() || namespaceInput.trim() === namespace
              }
            >
              Apply
              <Icon name="arrow" />
            </button>
          </form>
        ) : (
          <span className="namespace-label">
            <Icon name="layers" />
            Namespace <strong>{namespace}</strong>
          </span>
        )}
        <div className="refresh-settings">
          <label htmlFor="console-refresh">Auto-refresh</label>
          <select
            id="console-refresh"
            value={refreshInterval}
            onChange={(event) => setRefreshInterval(Number(event.target.value))}
          >
            {intervals.map((interval) => (
              <option key={interval} value={interval}>
                {interval ? `Every ${interval}s` : "Off"}
              </option>
            ))}
          </select>
        </div>
      </div>

      <section className="stats-grid" aria-label="Loaded application counts">
        <div className="stat-card">
          <span className="stat-label">
            Loaded applications
            <Icon name="layers" />
          </span>
          <strong>{initialLoading ? "—" : apps.length}</strong>
          <span className="stat-note">
            Across loaded pages{config.historyEnabled ? " and history" : ""}
          </span>
        </div>
        <div className="stat-card">
          <span className="stat-label">
            Running
            <span className="stat-marker marker-running" />
          </span>
          <strong>{initialLoading ? "—" : running}</strong>
          <span className="stat-note">Currently processing</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">
            Pending
            <span className="stat-marker marker-pending" />
          </span>
          <strong>{initialLoading ? "—" : pending}</strong>
          <span className="stat-note">Waiting to start</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">
            Failed
            <span className="stat-marker marker-failed" />
          </span>
          <strong>{initialLoading ? "—" : failed}</strong>
          <span className="stat-note">May need your attention</span>
        </div>
      </section>

      {feedback && (
        <div className="notice notice-success" role="status">
          <Icon name="check" />
          <span>{feedback}</span>
          <button
            className="icon-button"
            aria-label="Dismiss notification"
            onClick={() => setFeedback(null)}
          >
            <Icon name="close" />
          </button>
        </div>
      )}
      {error && (
        <div className="notice notice-error" role="alert">
          <Icon name="alert" />
          <span>
            <strong>Applications could not be refreshed.</strong> {error}
          </span>
          <button className="text-link" onClick={() => void loadApps()}>
            Try again
          </button>
        </div>
      )}
      {historyError && (
        <div className="notice notice-warning" role="status">
          <Icon name="alert" />
          <span>
            <strong>History is unavailable.</strong> Live applications are still
            shown. {historyError}
          </span>
          <button className="text-link" onClick={() => void loadHistory()}>
            Retry history
          </button>
        </div>
      )}

      <section className="applications-card" aria-label="Applications">
        <div className="table-toolbar">
          <div className="section-title">
            <h2>Workloads</h2>
            <span className="count-pill">{apps.length} loaded</span>
          </div>
          <div className="table-filters">
            <label className="search-field">
              <Icon name="search" />
              <span className="sr-only">Search loaded applications</span>
              <input
                type="search"
                placeholder="Search applications…"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
              />
            </label>
            <label className="status-select">
              <span className="sr-only">Filter by status</span>
              <select
                value={statusFilter}
                onChange={(event) => setStatusFilter(event.target.value)}
              >
                <option value="all">All statuses</option>
                {statuses.map((status) => (
                  <option key={status} value={status}>
                    {status}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>
        {initialLoading ? (
          <div className="empty-state" role="status">
            <span className="loading-ring" />
            <h3>Loading applications</h3>
            <p>Checking the workloads in {namespace}.</p>
          </div>
        ) : filtered.length === 0 ? (
          <div className="empty-state">
            <div className="empty-icon">
              <Icon
                name={query || statusFilter !== "all" ? "search" : "layers"}
              />
            </div>
            <h3>
              {query || statusFilter !== "all"
                ? "No matching applications"
                : error
                  ? "Applications are unavailable"
                  : "A little quiet here"}
            </h3>
            <p>
              {query || statusFilter !== "all"
                ? "Try a different search or status. Filters apply to loaded applications."
                : error
                  ? "Try refreshing the list when the connection is available."
                  : `No applications have been found in ${namespace}${config.historyEnabled ? " or the loaded history" : ""}.`}
            </p>
            {(query || statusFilter !== "all") && (
              <button
                className="button button-secondary"
                onClick={() => {
                  setQuery("");
                  setStatusFilter("all");
                }}
              >
                Clear filters
              </button>
            )}
          </div>
        ) : (
          <div className="table-scroll">
            <table className="application-table">
              <caption className="sr-only">
                Loaded Spark applications in {namespace}; archived applications
                may have no namespace.
              </caption>
              <thead>
                <tr>
                  <th scope="col">Application</th>
                  <th scope="col">Status</th>
                  <th scope="col" className="column-created">
                    Created
                  </th>
                  <th scope="col" className="column-driver">
                    Driver
                  </th>
                  <th scope="col">Quick links</th>
                  <th scope="col">
                    <span className="sr-only">Details</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((app) => (
                  <tr key={app.app_id}>
                    <td>
                      <button
                        className="application-name"
                        onClick={() => setSelectedId(app.app_id)}
                      >
                        {app.name || app.app_id}
                      </button>
                      <div className="application-meta">
                        <span className="mono" title={app.app_id}>
                          {app.app_id}
                        </span>
                        {app.source === "history" && (
                          <span className="archive-label">
                            {config.embedded
                              ? "Driver removed · task reference"
                              : "Archived · namespace unknown"}
                          </span>
                        )}
                      </div>
                    </td>
                    <td>
                      <StatusBadge status={app.status} />
                    </td>
                    <td className="column-created">
                      <time
                        dateTime={app.created_at || undefined}
                        title={app.created_at || undefined}
                      >
                        {dateLabel(app.created_at)}
                      </time>
                    </td>
                    <td className="column-driver">
                      <span
                        className="driver-label mono"
                        title={app.driver_pod || undefined}
                      >
                        {app.driver_pod || "—"}
                      </span>
                      <span className="muted cell-secondary">
                        {app.spark_version
                          ? `Spark ${app.spark_version}`
                          : app.source === "history"
                            ? config.embedded
                              ? "Task reference"
                              : "History server"
                            : "Kubernetes"}
                      </span>
                    </td>
                    <td>
                      <AppLinks
                        app={app}
                        config={config}
                        transport={transport}
                        onOpenLogs={onOpenLogs}
                      />
                    </td>
                    <td>
                      <button
                        className="icon-button"
                        aria-label={`View details for ${app.name || app.app_id}`}
                        onClick={() => setSelectedId(app.app_id)}
                      >
                        <Icon name="arrow" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="table-footer">
          <span>
            {initialLoading
              ? "Loading…"
              : `Showing ${filtered.length} of ${apps.length} loaded applications`}
            {historyLoading && !initialLoading ? " · Loading history…" : ""}
          </span>
          {cursor && (
            <button
              className="button button-secondary button-small"
              disabled={loadingMore || refreshing}
              onClick={() => void loadApps(true, cursor)}
            >
              {loadingMore ? "Loading…" : "Load more"}
              <Icon name="arrow" />
            </button>
          )}
        </div>
      </section>

      <div className="page-footnote">
        <span>
          <Icon name="clock" />
          {updatedAt
            ? `Updated ${updatedAt.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`
            : "Waiting for the first update"}
        </span>
        <span>Counts reflect loaded applications, not the entire cluster.</span>
      </div>

      {selected && !confirmation && (
        <Dialog
          title={selected.name || selected.app_id}
          onClose={() => setSelectedId(null)}
        >
          <div className="detail-summary">
            <StatusBadge status={selected.status} />
            <span className="source-label">
              {selected.source === "history"
                ? config.embedded
                  ? "Driver removed · task reference"
                  : "Archived application"
                : "Kubernetes application"}
            </span>
          </div>
          <dl className="detail-grid">
            {[
              ["Application ID", selected.app_id],
              [
                "Namespace",
                selected.namespace || "Unknown · archived application",
              ],
              ["Created", dateLabel(selected.created_at)],
              ["Driver pod", selected.driver_pod],
              ["Image", selected.image],
              ["Spark version", selected.spark_version],
              ["Node", selected.node_name],
            ].map(([label, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>{value || "—"}</dd>
              </div>
            ))}
          </dl>
          <div className="detail-links">
            <AppLinks
              app={selected}
              config={config}
              transport={transport}
              onOpenLogs={onOpenLogs}
            />
          </div>
          <Diagnostics app={selected} transport={transport} />
          {canManage(selected) &&
            ((config.allowKill && selected.status === "Running") ||
              config.allowDelete) && (
              <div className="dialog-actions destructive-actions">
                {config.allowKill && selected.status === "Running" && (
                  <button
                    className="button button-secondary"
                    onClick={() => confirm(selected, "kill")}
                  >
                    <Icon name="stop" />
                    Stop application
                  </button>
                )}
                {config.allowDelete && (
                  <button
                    className="button button-danger-outline"
                    onClick={() => confirm(selected, "delete")}
                  >
                    <Icon name="trash" />
                    Delete application
                  </button>
                )}
              </div>
            )}
        </Dialog>
      )}

      {confirmation && (
        <Dialog
          title={
            confirmation.action === "kill"
              ? "Stop this application?"
              : "Delete this application?"
          }
          onClose={() => {
            if (!actionBusy) setConfirmation(null);
          }}
          className="confirmation-dialog"
        >
          <div className="confirmation-icon">
            <Icon name={confirmation.action === "kill" ? "stop" : "trash"} />
          </div>
          <p>
            <strong>{confirmation.app.name || confirmation.app.app_id}</strong>{" "}
            in <strong>{confirmation.app.namespace}</strong>
          </p>
          <p className="muted">
            {confirmation.action === "kill"
              ? "This stops the running driver. The application will stop processing, and its existing resources remain available for inspection."
              : "This removes the driver pod and its dependent resources. This action cannot be undone."}
          </p>
          {actionError && (
            <div className="notice notice-error" role="alert">
              {actionError}
            </div>
          )}
          <div className="dialog-actions">
            <button
              className="button button-secondary"
              disabled={actionBusy}
              onClick={() => setConfirmation(null)}
            >
              Cancel
            </button>
            <button
              className="button button-danger"
              disabled={actionBusy}
              onClick={() => void performAction()}
            >
              {actionBusy
                ? "Working…"
                : confirmation.action === "kill"
                  ? "Stop application"
                  : "Delete application"}
            </button>
          </div>
        </Dialog>
      )}
    </>
  );
}
