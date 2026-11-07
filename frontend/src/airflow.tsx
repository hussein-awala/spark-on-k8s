import { useEffect, useMemo, useRef, useState } from "react";
import { SparkConsole } from "./SparkConsole";
import { checkResponse, errorMessage } from "./client";
import type { ConsoleApplication, ConsoleTransport, Diagnosis } from "./types";
import "./styles.css";

interface AirflowProps {
  dagId?: string;
  runId?: string;
  taskId?: string;
  mapIndex?: string | number;
  taskInstance?: {
    try_number: number;
    region_id?: string | null;
    region_index?: number | null;
  };
}
interface Capabilities {
  namespace: string;
  can_kill: boolean;
  can_delete: boolean;
  spark_ui_url: string | null;
  spark_history_url: string | null;
}

function taskQuery(props: AirflowProps): string | null {
  const { dagId, runId, taskId, taskInstance, mapIndex } = props;
  if (
    !dagId ||
    !runId ||
    !taskId ||
    !taskInstance ||
    taskInstance.try_number < 1
  )
    return null;
  const query = new URLSearchParams({
    dag_id: dagId,
    run_id: runId,
    task_id: taskId,
    try_number: String(taskInstance.try_number),
  });
  if (taskInstance.region_id && taskInstance.region_index != null) {
    query.set("region_id", taskInstance.region_id);
    query.set("region_index", String(taskInstance.region_index));
  } else {
    query.set("map_index", String(mapIndex ?? -1));
  }
  return query.toString();
}

function pluginBase(): URL {
  return new URL("spark-on-k8s/", document.baseURI);
}

function safeLink(value: string | null): string | null {
  if (!value) return null;
  try {
    const url = new URL(value);
    return ["https:", "http:"].includes(url.protocol) ? url.href : null;
  } catch {
    return null;
  }
}

function TaskConsole({ query, label }: { query: string; label: string }) {
  const [capabilities, setCapabilities] = useState<Capabilities | null>(null);
  const capabilitiesRef = useRef<Capabilities | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const adapter = useMemo(() => {
    const endpoint = (path: string) => {
      const url = new URL(`api/${path}`, pluginBase());
      url.search = query;
      return url;
    };
    async function request(path: string, options: RequestInit = {}) {
      return checkResponse(
        await fetch(endpoint(path), {
          ...options,
          credentials: "same-origin",
        }),
      );
    }
    async function refreshCapabilities(signal?: AbortSignal) {
      const response = await request("capabilities", { signal });
      const next = (await response.json()) as Capabilities;
      if (!signal?.aborted) {
        capabilitiesRef.current = next;
        setCapabilities(next);
      }
      return next;
    }
    const transport: ConsoleTransport = {
      async diagnoseApp(_app, signal) {
        const response = await request("diagnostics", { signal });
        return ((await response.json()) as { diagnosis: Diagnosis }).diagnosis;
      },
      async listApps({ signal }) {
        const [response] = await Promise.all([
          request("application", { signal }),
          refreshCapabilities(signal),
        ]);
        const application = (await response.json()) as ConsoleApplication;
        return { items: [application], next_cursor: null };
      },
      async listHistory({ signal }) {
        const response = await request("history", { signal });
        return ((await response.json()) as { items: ConsoleApplication[] })
          .items;
      },
      async killApp() {
        await request("kill", {
          method: "POST",
          headers: { "X-Spark-On-K8s-Action": "true" },
        });
      },
      async deleteApp() {
        await request("application", {
          method: "DELETE",
          headers: { "X-Spark-On-K8s-Action": "true" },
        });
      },
      openLogs(_namespace, _appId, tail) {
        const url = endpoint("logs");
        url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
        url.searchParams.set("tail", String(tail));
        return new WebSocket(url);
      },
      sparkUiUrl: () => safeLink(capabilitiesRef.current?.spark_ui_url ?? null),
      historyUrl: () =>
        safeLink(capabilitiesRef.current?.spark_history_url ?? null),
    };
    return { transport, refreshCapabilities };
  }, [query]);

  useEffect(() => {
    const controller = new AbortController();
    setError(null);
    void adapter
      .refreshCapabilities(controller.signal)
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) setError(errorMessage(reason));
      });
    return () => controller.abort();
  }, [adapter, attempt]);

  if (error)
    return (
      <div className="spark-console console-embedded">
        <div className="console-content">
          <p role="alert">{error}</p>
          <button
            className="button button-secondary"
            onClick={() => setAttempt((value) => value + 1)}
          >
            Retry
          </button>
        </div>
      </div>
    );
  if (!capabilities)
    return (
      <div className="spark-console console-embedded">
        <p role="status">Loading Spark task…</p>
      </div>
    );
  return (
    <SparkConsole
      transport={adapter.transport}
      config={{
        basePath: "",
        namespace: capabilities.namespace,
        refreshInterval: 10,
        allowKill: capabilities.can_kill,
        allowDelete: capabilities.can_delete,
        historyEnabled: Boolean(capabilities.spark_history_url),
        version: "",
        view: "apps",
        embedded: true,
        namespaceEditable: false,
        contextLabel: label,
      }}
    />
  );
}

export function AirflowSparkConsole(props: AirflowProps) {
  useEffect(() => {
    const href = new URL("static/console.css", pluginBase()).href;
    if (!document.querySelector("link[data-spark-task-styles]")) {
      const link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = href;
      link.dataset.sparkTaskStyles = "true";
      document.head.append(link);
    }
  }, []);
  const query = taskQuery(props);
  if (!query)
    return (
      <div className="spark-console console-embedded">
        <p role="status">Open a task attempt to view its Spark application.</p>
      </div>
    );
  return (
    <TaskConsole
      key={query}
      query={query}
      label={`${props.dagId} / ${props.taskId}`}
    />
  );
}

// Airflow mounts this component in its own React tree and supplies task context.
(globalThis as unknown as Record<string, unknown>)["Spark on Kubernetes"] =
  AirflowSparkConsole;
