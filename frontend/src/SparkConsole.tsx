import { useEffect, useMemo, useRef, useState } from "react";
import { Applications } from "./Applications";
import { consolePath, createConsoleTransport } from "./client";
import { Icon, SparkMark } from "./icons";
import { Logs } from "./Logs";
import { Runs } from "./Runs";
import type {
  ConsoleApplication,
  ConsoleConfig,
  ConsoleTransport,
} from "./types";

interface SparkConsoleProps {
  config: ConsoleConfig;
  transport?: ConsoleTransport;
  namespaceEditable?: boolean;
}

export function SparkConsole(props: SparkConsoleProps) {
  try {
    consolePath(props.config.basePath, "/");
  } catch {
    return (
      <div className="spark-console console-embedded">
        <div className="console-content">
          <p role="alert">
            The console base path is invalid. Check the deployment
            configuration.
          </p>
        </div>
      </div>
    );
  }
  return <ConsoleContent {...props} />;
}

function ConsoleContent({
  config,
  transport: suppliedTransport,
  namespaceEditable = config.namespaceEditable ?? true,
}: SparkConsoleProps) {
  const transport = useMemo(
    () =>
      suppliedTransport ??
      createConsoleTransport({ basePath: config.basePath }),
    [suppliedTransport, config.basePath],
  );
  const [location, setLocation] = useState({
    view: config.view,
    namespace: config.namespace,
    appId: config.appId,
  });
  useEffect(() => {
    setLocation({
      view: config.view,
      namespace: config.namespace,
      appId: config.appId,
    });
  }, [config.view, config.namespace, config.appId]);
  const content = useRef<HTMLDivElement>(null);
  const navigateFocus = useRef(false);
  const logsOrigin = useRef<string | undefined>(undefined);
  const [restoreLogsFor, setRestoreLogsFor] = useState<string | undefined>();
  useEffect(() => {
    if (!navigateFocus.current) return;
    navigateFocus.current = false;
    content.current?.querySelector<HTMLElement>("h1")?.focus();
  }, [location.view]);
  const activeConfig = { ...config, ...location };
  function openLogs(app: ConsoleApplication) {
    navigateFocus.current = true;
    logsOrigin.current = app.app_id;
    setLocation({
      view: "logs",
      namespace: app.namespace || config.namespace,
      appId: app.app_id,
    });
  }
  function backToApps() {
    navigateFocus.current = true;
    setRestoreLogsFor(logsOrigin.current);
    setLocation({
      view: "apps",
      namespace: location.namespace,
      appId: undefined,
    });
  }
  function changeNamespace(namespace: string) {
    setLocation((previous) => ({ ...previous, namespace }));
    if (!config.embedded) {
      const url = new URL(window.location.href);
      url.searchParams.set("namespace", namespace);
      window.history.replaceState(window.history.state, "", url);
    }
  }
  return (
    <div
      className={`spark-console ${config.embedded ? "console-embedded" : "console-standalone"}`}
    >
      {!config.embedded && (
        <aside className="console-sidebar" aria-label="Console navigation">
          <a
            className="brand"
            href={consolePath(
              config.basePath,
              `/webserver/apps?namespace=${encodeURIComponent(location.namespace)}`,
            )}
          >
            <SparkMark />
            <span>
              spark<span className="brand-subtitle">ON KUBERNETES</span>
            </span>
          </a>
          <div className="sidebar-section-label">WORKSPACE</div>
          <nav>
            <a
              className={`sidebar-link ${location.view === "apps" ? "sidebar-link-active" : ""}`}
              aria-current={location.view === "apps" ? "page" : undefined}
              href={consolePath(
                config.basePath,
                `/webserver/apps?namespace=${encodeURIComponent(location.namespace)}`,
              )}
            >
              <Icon name="grid" />
              Applications
            </a>
            {config.runsEnabled && (
              <a
                className={`sidebar-link ${location.view === "runs" ? "sidebar-link-active" : ""}`}
                aria-current={location.view === "runs" ? "page" : undefined}
                href={consolePath(
                  config.basePath,
                  `/webserver/runs?namespace=${encodeURIComponent(location.namespace)}`,
                )}
              >
                <Icon name="layers" />
                Recorded runs
              </a>
            )}
            {location.view === "logs" && (
              <span
                className="sidebar-link sidebar-link-active"
                aria-current="page"
              >
                <Icon name="terminal" />
                Driver logs
              </span>
            )}
          </nav>
          <div className="sidebar-footer">
            <span className="sidebar-status">
              <Icon name="layers" />
              Kubernetes console
            </span>
            <span className="sidebar-version">v{config.version}</span>
          </div>
        </aside>
      )}
      <main className="console-main">
        <div className="console-content" ref={content}>
          {location.view === "runs" ? (
            <Runs
              config={activeConfig}
              transport={transport}
              namespaceEditable={namespaceEditable}
              onNamespaceChange={changeNamespace}
            />
          ) : location.view === "logs" ? (
            <Logs
              config={activeConfig}
              transport={transport}
              onBack={backToApps}
            />
          ) : (
            <Applications
              restoreLogsFor={restoreLogsFor}
              onFocusRestored={() => setRestoreLogsFor(undefined)}
              config={activeConfig}
              transport={transport}
              namespaceEditable={namespaceEditable}
              onOpenLogs={openLogs}
              onNamespaceChange={changeNamespace}
            />
          )}
        </div>
        <footer className="console-footer">
          <span>Spark on Kubernetes</span>
          <a
            href="https://hussein.awala.fr/spark-on-k8s/"
            target="_blank"
            rel="noopener noreferrer"
          >
            Documentation
            <Icon name="external" />
          </a>
        </footer>
      </main>
    </div>
  );
}
