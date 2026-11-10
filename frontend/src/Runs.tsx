import { useCallback, useEffect, useRef, useState } from "react";
import { StatusBadge } from "./Applications";
import { errorMessage } from "./client";
import { Dialog } from "./Dialog";
import { Icon } from "./icons";
import type {
  ConsoleConfig,
  ConsoleTransport,
  RecordedRun,
  RunCollection,
} from "./types";

function date(value: string | null) {
  if (!value) return "—";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? "—" : parsed.toLocaleString();
}

export function Runs({
  config,
  transport,
  namespaceEditable,
  onNamespaceChange,
}: {
  config: ConsoleConfig;
  transport: ConsoleTransport;
  namespaceEditable: boolean;
  onNamespaceChange: (namespace: string) => void;
}) {
  const [namespaceInput, setNamespaceInput] = useState(config.namespace);
  const [status, setStatus] = useState("");
  const [presence, setPresence] = useState<"all" | "present" | "missing">(
    "all",
  );
  const [items, setItems] = useState<RecordedRun[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [errorCursor, setErrorCursor] = useState<string | undefined>();
  const [selected, setSelected] = useState<RecordedRun | null>(null);
  const [collection, setCollection] = useState<RunCollection | null>(null);
  const request = useRef<AbortController | null>(null);
  const enabled = Boolean(config.runsEnabled && transport.listRuns);

  const load = useCallback(
    async (next?: string) => {
      request.current?.abort();
      const controller = new AbortController();
      request.current = controller;
      if (!enabled || !transport.listRuns) return;
      setLoading(true);
      setError(null);
      // Collection freshness is advisory: a failed lookup never hides records.
      if (!next && transport.runsConfig)
        transport
          .runsConfig(controller.signal)
          .then((settings) => {
            if (controller.signal.aborted) return;
            setCollection(
              settings.collection?.find(
                (item) => item.namespace === config.namespace,
              ) ?? null,
            );
          })
          .catch(() => {
            if (!controller.signal.aborted) setCollection(null);
          });
      try {
        const page = await transport.listRuns({
          namespace: config.namespace,
          status,
          presence,
          limit: 50,
          cursor: next,
          signal: controller.signal,
        });
        if (controller.signal.aborted) return;
        setItems((previous) => {
          const rows = new Map(
            (next ? previous : []).map((run) => [
              `${run.namespace}/${run.uid}`,
              run,
            ]),
          );
          for (const run of page.items)
            rows.set(`${run.namespace}/${run.uid}`, run);
          return [...rows.values()];
        });
        setCursor(page.next_cursor);
      } catch (failure) {
        if (!controller.signal.aborted) {
          setError(errorMessage(failure));
          setErrorCursor(next);
        }
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    },
    [config.namespace, status, presence, transport, enabled],
  );

  useEffect(() => {
    setItems([]);
    setCursor(null);
    setSelected(null);
    setNamespaceInput(config.namespace);
    void load();
    return () => request.current?.abort();
  }, [load, config.namespace]);

  if (!enabled)
    return (
      <div className="empty-state" role="status">
        <h1>Run history is not available</h1>
        <p>Ask the service administrator to enable the metadata recorder.</p>
      </div>
    );

  return (
    <>
      <header className="page-heading">
        <div>
          <div className="eyebrow">SPARK ON KUBERNETES</div>
          <h1>
            Recorded runs<span className="heading-dot">.</span>
          </h1>
          <p className="page-description">
            Keep a record after pods are removed. Status is the last observed
            phase.
          </p>
        </div>
        <button
          className="button button-secondary refresh-button"
          disabled={loading}
          onClick={() => void load()}
        >
          <Icon name="refresh" className={loading ? "spinning" : ""} />
          Refresh
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
            <label htmlFor="runs-namespace">
              <Icon name="layers" />
              Namespace
            </label>
            <input
              id="runs-namespace"
              value={namespaceInput}
              onChange={(event) => setNamespaceInput(event.target.value)}
              required
              maxLength={63}
              pattern="[a-z0-9](([a-z0-9]|-)*[a-z0-9])?"
              autoComplete="off"
              spellCheck={false}
            />
            <button
              className="button button-small button-secondary"
              type="submit"
              disabled={
                !namespaceInput.trim() ||
                namespaceInput.trim() === config.namespace
              }
            >
              Apply
            </button>
          </form>
        ) : (
          <span className="namespace-label">
            Namespace <strong>{config.namespace}</strong>
          </span>
        )}
        <span className="muted">Read-only metadata · refreshed on request</span>
      </div>
      {collection?.namespace === config.namespace && !collection.fresh && (
        <div className="notice notice-warning" role="status">
          <Icon name="alert" />
          <span>
            <strong>Collection for {collection.namespace} is stale.</strong>{" "}
            Records may not reflect current pods. Last successful collection:{" "}
            {collection.last_success ? date(collection.last_success) : "never"}
            {collection.last_error &&
              ` · last error: ${collection.last_error.replace(/_/g, " ")}`}
            .
          </span>
        </div>
      )}
      <section className="applications-card" aria-label="Recorded runs">
        <div className="table-toolbar">
          <h2>Run history</h2>
          <div className="table-filters runs-filters">
            <label htmlFor="runs-status">Last status</label>
            <select
              id="runs-status"
              value={status}
              onChange={(event) => setStatus(event.target.value)}
            >
              <option value="">All statuses</option>
              {["Pending", "Running", "Succeeded", "Failed", "Unknown"].map(
                (value) => (
                  <option key={value}>{value}</option>
                ),
              )}
            </select>
            <label htmlFor="runs-presence">Pod observation</label>
            <select
              id="runs-presence"
              value={presence}
              onChange={(event) =>
                setPresence(event.target.value as typeof presence)
              }
            >
              <option value="all">All records</option>
              <option value="present">Seen in latest collection</option>
              <option value="missing">No longer observed</option>
            </select>
          </div>
        </div>
        {error && (
          <div className="notice notice-error" role="alert">
            <p>{error}</p>
            <button
              className="button button-secondary"
              disabled={loading}
              onClick={() => void load(errorCursor)}
            >
              Retry
            </button>
          </div>
        )}
        {loading && !items.length && (
          <div className="empty-state" role="status">
            <span className="loading-ring" />
            Loading recorded runs…
          </div>
        )}
        {!loading && !error && !items.length && (
          <div className="empty-state" role="status">
            <h3>No recorded runs</h3>
            <p>
              Try another filter or refresh after the recorder observes a driver
              pod.
            </p>
          </div>
        )}
        {items.length > 0 && (
          <div className="table-scroll">
            <table className="application-table">
              <caption className="sr-only">
                Recorded runs in {config.namespace}; status is the last observed
                phase.
              </caption>
              <thead>
                <tr>
                  <th>Application</th>
                  <th>Last status</th>
                  <th>Pod observation</th>
                  <th>First seen</th>
                  <th>Last seen</th>
                </tr>
              </thead>
              <tbody>
                {items.map((run) => (
                  <tr key={`${run.namespace}/${run.uid}`}>
                    <td>
                      <button
                        className="application-name"
                        onClick={() => setSelected(run)}
                      >
                        {run.name}
                      </button>
                      <div className="application-meta mono">
                        {run.app_id || run.driver_pod}
                      </div>
                    </td>
                    <td>
                      <StatusBadge status={run.status} />
                    </td>
                    <td>
                      {run.disappeared_at
                        ? "No longer observed"
                        : "Seen in latest collection"}
                    </td>
                    <td>{date(run.first_seen)}</td>
                    <td>{date(run.last_seen)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="table-footer">
          <span>
            {items.length} recorded {items.length === 1 ? "run" : "runs"} loaded
          </span>
          {cursor && (
            <button
              className="button button-secondary"
              disabled={loading}
              onClick={() => void load(cursor)}
            >
              {loading ? "Loading…" : "Load more"}
            </button>
          )}
        </div>
      </section>
      {selected && (
        <Dialog title={selected.name} onClose={() => setSelected(null)}>
          <p className="notice">
            This is a recorded snapshot. A missing pod does not establish that
            its application failed.
          </p>
          <dl className="run-details">
            {[
              ["Run identity", selected.uid],
              ["Namespace", selected.namespace],
              ["Application ID", selected.app_id],
              ["Driver pod", selected.driver_pod],
              ["Last status", selected.status],
              ["Image", selected.image],
              ["Spark version", selected.spark_version],
              ["Created", date(selected.created_at)],
              ["First seen", date(selected.first_seen)],
              ["Last seen", date(selected.last_seen)],
              ["No longer observed since", date(selected.disappeared_at)],
            ].map(([label, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>{value || "—"}</dd>
              </div>
            ))}
          </dl>
        </Dialog>
      )}
    </>
  );
}
