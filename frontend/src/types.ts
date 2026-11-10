export interface ConsoleConfig {
  basePath: string;
  namespace: string;
  refreshInterval: number;
  allowKill: boolean;
  allowDelete: boolean;
  historyEnabled: boolean;
  runsEnabled?: boolean;
  version: string;
  view: "apps" | "logs" | "runs";
  appId?: string;
  tail?: number;
  embedded?: boolean;
  namespaceEditable?: boolean;
  /** Namespaces the server allows; null or absent when any namespace may be entered. */
  namespaces?: string[] | null;
  contextLabel?: string;
  /**
   * Stop automatic refresh once every loaded live application has succeeded or
   * failed. For views of one task's application; a namespace view can gain new
   * applications at any time and keeps refreshing.
   */
  stopRefreshWhenFinished?: boolean;
}

export interface ConsoleApplication {
  app_id: string;
  name: string | null;
  namespace: string | null;
  status: string;
  created_at: string | null;
  driver_pod: string | null;
  image: string | null;
  spark_version: string | null;
  node_name: string | null;
  driver_logs: boolean;
  spark_ui_proxy: boolean;
  spark_history_proxy: boolean;
  source: "kubernetes" | "history";
}

export interface ApplicationPage {
  items: ConsoleApplication[];
  next_cursor: string | null;
}

export interface Diagnosis {
  schema_version: number;
  namespace: string;
  app_id: string;
  driver_pod: string;
  incomplete: boolean;
  inspected_pods: number;
  inspected_events: number;
  findings: {
    code: string;
    severity: "info" | "warning" | "error";
    resource: string;
    container: string | null;
    summary: string;
    evidence: string;
    remediation: string;
  }[];
}

export interface RecordedRun {
  uid: string;
  app_id: string | null;
  name: string;
  namespace: string;
  driver_pod: string;
  image: string | null;
  spark_version: string | null;
  status: string;
  created_at: string | null;
  first_seen: string;
  last_seen: string;
  disappeared_at: string | null;
}

export interface RunPage {
  schema_version: 1;
  items: RecordedRun[];
  next_cursor: string | null;
}

export interface RunCollection {
  namespace: string;
  fresh: boolean;
  last_success: string | null;
  last_attempt: string | null;
  last_error: string | null;
}

export interface RunHistoryConfig {
  schema_version: 1;
  namespaces: string[];
  collection?: RunCollection[];
}

export interface ConsoleTransport {
  runsConfig?(signal?: AbortSignal): Promise<RunHistoryConfig>;
  listRuns?(options: {
    namespace: string;
    status?: string;
    presence?: "all" | "present" | "missing";
    limit: number;
    cursor?: string;
    signal?: AbortSignal;
  }): Promise<RunPage>;

  diagnoseApp?(
    app: ConsoleApplication,
    signal?: AbortSignal,
  ): Promise<Diagnosis>;
  listApps(options: {
    namespace: string;
    limit: number;
    cursor?: string;
    signal?: AbortSignal;
  }): Promise<ApplicationPage>;
  listHistory(options: { signal?: AbortSignal }): Promise<ConsoleApplication[]>;
  killApp(namespace: string, appId: string): Promise<void>;
  deleteApp(namespace: string, appId: string): Promise<void>;
  openLogs(namespace: string, appId: string, tail: number): WebSocket;
  sparkUiUrl?(app: ConsoleApplication): string | null;
  historyUrl?(app: ConsoleApplication): string | null;
}
