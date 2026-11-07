import { createRoot } from "react-dom/client";
import {
  SparkConsole,
  createConsoleTransport,
  type ConsoleConfig,
} from "../src/index";
import { archiveApp, demoApps } from "./data";

const params = new URLSearchParams(window.location.search);
const config: ConsoleConfig = {
  basePath: params.get("basePath") ?? "/nested/spark",
  namespace: params.get("namespace") ?? "analytics",
  refreshInterval: Number(params.get("refresh") ?? 0),
  allowKill: params.get("allowKill") !== "false",
  allowDelete: params.get("allowDelete") !== "false",
  runsEnabled: params.get("runs") === "true",
  historyEnabled: params.get("history") !== "false",
  version: "0.11.0",
  view:
    params.get("view") === "runs"
      ? "runs"
      : params.get("view") === "logs"
        ? "logs"
        : "apps",
  appId: params.get("appId") ?? "report-1",
  tail: Number(params.get("tail") ?? 1000),
  embedded: params.get("embedded") === "true",
  namespaceEditable: params.get("namespaceEditable") !== "false",
  contextLabel: params.get("contextLabel") ?? undefined,
};
const transport = createConsoleTransport(config);
if (params.get("demo") === "true") {
  transport.listApps = async () => ({ items: demoApps, next_cursor: null });
  transport.listHistory = async () => [archiveApp];
  transport.openLogs = () => {
    const socket = new EventTarget();
    const timer = window.setTimeout(() => {
      socket.dispatchEvent(new Event("open"));
      const messages = [
        "26/11/07 09:12:01 INFO SparkContext: Running Spark version 3.5.9",
        "26/11/07 09:12:01 INFO SparkContext: Submitted application: customer-daily-rollup",
        "26/11/07 09:12:02 INFO KubernetesClusterSchedulerBackend: Scheduler backend is ready",
        "26/11/07 09:12:03 INFO SharedState: Warehouse path is 'file:/opt/spark/warehouse'",
        "26/11/07 09:12:04 INFO FileSourceScanExec: Planning scan with bin packing",
        "26/11/07 09:12:05 INFO DAGScheduler: Got job 0 with 128 output partitions",
        "26/11/07 09:12:05 INFO DAGScheduler: Submitting 128 missing tasks from ResultStage 0",
        "26/11/07 09:12:06 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (executor 1)",
        "26/11/07 09:12:07 INFO TaskSetManager: Finished task 0.0 in stage 0.0 in 842 ms",
        "26/11/07 09:12:08 INFO Executor: Result sent to driver; 118 / 128 tasks complete",
        "26/11/07 09:12:09 INFO TaskSetManager: Finished task 127.0 in stage 0.0 in 711 ms",
        "26/11/07 09:12:10 INFO DAGScheduler: ResultStage 0 finished in 5.108 s",
      ];
      for (const line of messages)
        socket.dispatchEvent(new MessageEvent("message", { data: line }));
    }, 30);
    return Object.assign(socket, {
      close() {
        clearTimeout(timer);
      },
    }) as WebSocket;
  };
}
if (params.get("authorizedLinks") === "true") {
  transport.sparkUiUrl = () => "/authorized/spark-ui?task=aggregate";
  transport.historyUrl = () => "/authorized/history?task=aggregate";
}
createRoot(document.getElementById("root")!).render(
  <SparkConsole config={config} transport={transport} />,
);
