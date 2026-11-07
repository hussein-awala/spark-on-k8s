export interface ConsoleConfig {
  basePath: string;
  namespace: string;
  refreshInterval: number;
  allowKill: boolean;
  allowDelete: boolean;
  historyEnabled: boolean;
  version: string;
  view: "apps" | "logs";
  appId?: string;
  tail?: number;
  embedded?: boolean;
  namespaceEditable?: boolean;
  contextLabel?: string;
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

export interface ConsoleTransport {
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
