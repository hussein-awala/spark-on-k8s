import * as React from "react";
import * as ReactJSXRuntime from "react/jsx-runtime";
import { createRoot } from "react-dom/client";

export interface TaskProps {
  dagId?: string;
  runId?: string;
  taskId?: string;
  mapIndex?: number;
  taskInstance?: {
    try_number?: number;
    region_id?: string;
    region_index?: number;
  };
}

declare global {
  interface Window {
    airflowFixture: {
      render: (props: TaskProps) => void;
      unmount: () => void;
    };
  }
}

const query = new URLSearchParams(window.location.search);
const base = document.createElement("base");
base.href = query.get("base") || "/airflow/";
document.head.append(base);
const hostGlobals = globalThis as unknown as Record<string, unknown>;
hostGlobals.React = React;
hostGlobals.ReactJSXRuntime = ReactJSXRuntime;
const script = document.createElement("script");
script.src = new URL("spark-on-k8s/static/airflow.js", document.baseURI).href;
const root = createRoot(document.getElementById("airflow-host-root")!);
script.addEventListener("load", () => {
  const component = hostGlobals[
    "Spark on Kubernetes"
  ] as React.ComponentType<TaskProps>;
  if (typeof component !== "function")
    throw new Error("The plugin did not register its native component");
  window.airflowFixture = {
    render: (props) => root.render(React.createElement(component, props)),
    unmount: () => root.render(null),
  };
  const props: TaskProps = {
    dagId: query.get("dagId") ?? "daily_sales",
    runId: query.get("runId") ?? "scheduled__2026-11-07T09:00:00+00:00",
    taskId: query.get("taskId") ?? "aggregate",
    mapIndex: Number(query.get("mapIndex") ?? -1),
    taskInstance: { try_number: Number(query.get("tryNumber") ?? 2) },
  };
  if (query.has("regionId")) {
    props.taskInstance!.region_id = query.get("regionId")!;
    props.taskInstance!.region_index = Number(query.get("regionIndex") ?? -1);
  }
  window.airflowFixture.render(query.has("missingContext") ? {} : props);
});
script.addEventListener("error", () => {
  root.render(
    <div role="alert">The built plugin bundle could not be loaded.</div>,
  );
});
document.head.append(script);
