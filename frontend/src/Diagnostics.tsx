import { useEffect, useState } from "react";
import { errorMessage } from "./client";
import type { ConsoleApplication, ConsoleTransport, Diagnosis } from "./types";

export function Diagnostics({
  app,
  transport,
}: {
  app: ConsoleApplication;
  transport: ConsoleTransport;
}) {
  const [report, setReport] = useState<Diagnosis | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const supported =
    app.source === "kubernetes" &&
    !!app.driver_pod &&
    !!app.namespace &&
    !!transport.diagnoseApp;
  useEffect(() => {
    const controller = new AbortController();
    setReport(null);
    setError(null);
    if (supported) {
      transport.diagnoseApp!(app, controller.signal).then(
        (result) => {
          if (!controller.signal.aborted) setReport(result);
        },
        (failure) => {
          if (!controller.signal.aborted) setError(errorMessage(failure));
        },
      );
    }
    return () => controller.abort();
  }, [
    app.app_id,
    app.driver_pod,
    app.namespace,
    supported,
    transport,
    attempt,
  ]);
  if (!supported) return null;
  return (
    <section className="diagnostics" aria-label="Application diagnostics">
      <h3>Diagnostics</h3>
      {!report && !error && (
        <p role="status">Checking pod states and events…</p>
      )}
      {error && (
        <div className="notice notice-error" role="alert">
          <p>{error}</p>
          <button
            className="button button-secondary"
            onClick={() => setAttempt((value) => value + 1)}
          >
            Retry diagnostics
          </button>
        </div>
      )}
      {report && (
        <>
          <p className="muted">
            {report.inspected_pods} pods · {report.inspected_events} events
            inspected
          </p>
          {report.incomplete && (
            <p className="notice" role="status">
              Some evidence was unavailable or exceeded collection limits.
            </p>
          )}
          {report.findings.length === 0 && (
            <p>No specific problems observed in this snapshot.</p>
          )}
          {report.findings.map((finding, index) => (
            <article
              className="diagnostic-finding"
              key={`${finding.code}-${finding.resource}-${finding.container ?? ""}-${index}`}
            >
              <h4>{finding.summary}</h4>
              <p className="muted">
                {finding.severity} · {finding.resource}
                {finding.container && ` / ${finding.container}`}
              </p>
              <p>{finding.evidence}</p>
              <p>
                <strong>Next step:</strong> {finding.remediation}
              </p>
            </article>
          ))}
        </>
      )}
    </section>
  );
}
