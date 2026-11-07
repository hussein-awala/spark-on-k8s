import type { ConsoleApplication } from "../src/types";

export function application(
  overrides: Partial<ConsoleApplication> = {},
): ConsoleApplication {
  return {
    app_id: "report-1",
    name: "Daily revenue report",
    namespace: "analytics",
    status: "Running",
    created_at: "2026-11-07T09:12:00Z",
    driver_pod: "report-1-driver",
    image: "spark:3.5.9",
    spark_version: "3.5.9",
    node_name: "worker-02",
    driver_logs: true,
    spark_ui_proxy: true,
    spark_history_proxy: false,
    source: "kubernetes",
    ...overrides,
  };
}

export const demoApps = [
  application({
    app_id: "customer-rollup-20261107",
    name: "customer-daily-rollup",
    driver_pod: "customer-rollup-20261107-driver",
  }),
  application({
    app_id: "warehouse-ingest-20261107",
    name: "warehouse-ingest",
    driver_pod: "warehouse-ingest-20261107-driver",
    created_at: "2026-11-07T08:45:00Z",
  }),
  application({
    app_id: "session-enrichment-20261107",
    name: "session-enrichment",
    status: "Pending",
    driver_pod: "session-enrichment-20261107-driver",
    created_at: "2026-11-07T08:30:00Z",
  }),
  application({
    app_id: "revenue-report-20261107",
    name: "revenue-report",
    status: "Succeeded",
    driver_pod: "revenue-report-20261107-driver",
    created_at: "2026-11-07T07:15:00Z",
    spark_history_proxy: true,
  }),
  application({
    app_id: "feature-backfill-20261107",
    name: "feature-backfill",
    status: "Failed",
    driver_pod: "feature-backfill-20261107-driver",
    created_at: "2026-11-07T06:40:00Z",
  }),
];

export const archiveApp = application({
  app_id: "archive-1",
  name: "Weekly archive",
  status: "Succeeded",
  source: "history",
  namespace: null,
  driver_pod: null,
  driver_logs: false,
  spark_ui_proxy: false,
  spark_history_proxy: true,
});
