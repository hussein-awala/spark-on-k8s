import { useEffect, useMemo, useRef, useState } from "react";
import { consolePath, errorMessage } from "./client";
import { Icon } from "./icons";
import type { ConsoleConfig, ConsoleTransport } from "./types";

export const MAX_LOG_LINES = 10_000;
type ConnectionState = "connecting" | "live" | "closed" | "error";
interface LogLine {
  number: number;
  text: string;
}

export function Logs({
  config,
  transport,
  onBack,
}: {
  config: ConsoleConfig;
  transport: ConsoleTransport;
  onBack: () => void;
}) {
  const [lines, setLines] = useState<LogLine[]>([]);
  const [tail, setTail] = useState(config.tail ?? 1000);
  const [query, setQuery] = useState("");
  const [follow, setFollow] = useState(true);
  const [wrap, setWrap] = useState(false);
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const [connectionMessage, setConnectionMessage] = useState("");
  const [session, setSession] = useState(0);
  const [dropped, setDropped] = useState(0);
  const [feedback, setFeedback] = useState<string | null>(null);
  const pending = useRef<string[]>([]);
  const pendingDropped = useRef(0);
  const frame = useRef<number | null>(null);
  const nextLine = useRef(1);
  const viewport = useRef<HTMLDivElement>(null);
  const allLines = useRef<LogLine[]>([]);
  const appId = config.appId || "";

  function clearLines() {
    if (frame.current !== null) cancelAnimationFrame(frame.current);
    frame.current = null;
    pending.current = [];
    pendingDropped.current = 0;
    nextLine.current = 1;
    allLines.current = [];
    setLines([]);
    setDropped(0);
  }

  useEffect(() => {
    clearLines();
    setConnection("connecting");
    setConnectionMessage("");
    setFeedback(null);
    if (!appId || !config.namespace) {
      setConnection("error");
      setConnectionMessage(
        "An application and namespace are required to view driver logs.",
      );
      return;
    }
    let active = true;
    let failed = false;
    let socket: WebSocket;
    function flush() {
      frame.current = null;
      if (!active) return;
      nextLine.current += pendingDropped.current;
      const additions = pending.current.map((text) => ({
        number: nextLine.current++,
        text,
      }));
      pending.current = [];
      const combined = [...allLines.current, ...additions];
      const discarded =
        pendingDropped.current + Math.max(0, combined.length - MAX_LOG_LINES);
      pendingDropped.current = 0;
      const remaining = combined.slice(-MAX_LOG_LINES);
      allLines.current = remaining;
      if (discarded) setDropped((count) => count + discarded);
      setLines(remaining);
    }
    function opened() {
      if (active) {
        setConnection("live");
        setConnectionMessage("");
      }
    }
    function message(event: MessageEvent) {
      if (!active) return;
      const text =
        typeof event.data === "string" ? event.data : String(event.data);
      const additions = text.split(/\r?\n/);
      // Kubernetes usually sends one line per message; preserve multiline payloads as literal text.
      const combined = [...pending.current, ...additions];
      if (combined.length > MAX_LOG_LINES)
        pendingDropped.current += combined.length - MAX_LOG_LINES;
      pending.current = combined.slice(-MAX_LOG_LINES);
      if (frame.current === null) frame.current = requestAnimationFrame(flush);
    }
    function errored() {
      if (!active) return;
      failed = true;
      setConnection("error");
      setConnectionMessage(
        "The log connection failed. Check your connection and try reconnecting.",
      );
    }
    function closed(event: CloseEvent) {
      if (!active) return;
      if (event.code === 1008) {
        setConnection("error");
        setConnectionMessage(
          "Logs are unavailable for this application, or you do not have access.",
        );
      } else if (
        event.code === 1011 ||
        (event.code !== 1000 && event.code !== 1005)
      ) {
        setConnection("error");
        setConnectionMessage(
          "The log stream was interrupted. Reconnect to try again.",
        );
      } else if (!failed) {
        setConnection("closed");
        setConnectionMessage(
          "The log stream has ended. Reconnect to load the latest available logs.",
        );
      }
      if (pending.current.length) {
        if (frame.current !== null) cancelAnimationFrame(frame.current);
        flush();
      }
    }
    try {
      socket = transport.openLogs(config.namespace, appId, tail);
    } catch (failure) {
      setConnection("error");
      setConnectionMessage(errorMessage(failure));
      return;
    }
    socket.addEventListener("open", opened);
    socket.addEventListener("message", message);
    socket.addEventListener("error", errored);
    socket.addEventListener("close", closed);
    return () => {
      active = false;
      socket.removeEventListener("open", opened);
      socket.removeEventListener("message", message);
      socket.removeEventListener("error", errored);
      socket.removeEventListener("close", closed);
      if (frame.current !== null) cancelAnimationFrame(frame.current);
      frame.current = null;
      pending.current = [];
      socket.close();
    };
  }, [appId, config.namespace, tail, session, transport]);

  const visible = useMemo(() => {
    const needle = query.toLowerCase();
    return needle
      ? lines.filter((line) => line.text.toLowerCase().includes(needle))
      : lines;
  }, [lines, query]);
  const text = visible.map((line) => line.text).join("\n");
  useEffect(() => {
    if (follow && viewport.current)
      viewport.current.scrollTop = viewport.current.scrollHeight;
  }, [visible, follow]);

  async function copyLogs() {
    try {
      await navigator.clipboard.writeText(text);
      setFeedback(
        `${visible.length.toLocaleString()} visible log lines copied.`,
      );
    } catch {
      setFeedback(
        "Copy is unavailable in this browser. You can download the logs instead.",
      );
    }
  }

  function downloadLogs() {
    const blob = new Blob(
      [allLines.current.map((line) => line.text).join("\n")],
      { type: "text/plain;charset=utf-8" },
    );
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${appId.replace(/[^a-zA-Z0-9_.-]/g, "_") || "application"}.log`;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 0);
  }

  const choices = [...new Set([100, 1000, 5000, MAX_LOG_LINES, -1, tail])];
  const appsLink = consolePath(
    config.basePath,
    `/webserver/apps?namespace=${encodeURIComponent(config.namespace)}`,
  );
  return (
    <>
      {config.embedded ? (
        <button className="back-link" onClick={onBack}>
          <Icon name="back" />
          Applications
        </button>
      ) : (
        <a className="back-link" href={appsLink}>
          <Icon name="back" />
          Applications
        </a>
      )}
      <header className="page-heading logs-heading">
        <div>
          <div className="eyebrow">DRIVER OUTPUT</div>
          <h1>
            Application logs<span className="heading-dot">.</span>
          </h1>
          <p className="log-identity">
            <span>{appId || "No application selected"}</span>
            <span className="identity-divider">/</span>
            <span>{config.namespace}</span>
          </p>
        </div>
        <span
          className={`connection-badge connection-${connection}`}
          role="status"
        >
          <span className="status-dot" />
          {connection === "live"
            ? "Live stream"
            : connection === "connecting"
              ? "Connecting"
              : connection === "closed"
                ? "Stream ended"
                : "Disconnected"}
        </span>
      </header>

      <div className="log-controls">
        <div className="log-controls-group">
          <label className="tail-control" htmlFor="console-tail">
            Initial lines
            <select
              id="console-tail"
              value={tail}
              onChange={(event) => setTail(Number(event.target.value))}
            >
              {choices.map((count) => (
                <option key={count} value={count}>
                  {count <= 0 ? "All available" : count.toLocaleString()}
                </option>
              ))}
            </select>
          </label>
          <button
            className={`button button-secondary ${follow ? "" : "button-active"}`}
            onClick={() => setFollow((value) => !value)}
            aria-pressed={!follow}
          >
            <Icon name={follow ? "pause" : "play"} />
            {follow ? "Pause auto-scroll" : "Resume auto-scroll"}
          </button>
        </div>
        <div className="log-controls-group">
          <button
            className="button button-secondary"
            onClick={() => setSession((value) => value + 1)}
            disabled={connection === "connecting"}
          >
            <Icon name="refresh" />
            Reconnect
          </button>
          <button
            className="button button-secondary"
            onClick={() => void copyLogs()}
            disabled={!visible.length}
          >
            <Icon name="copy" />
            Copy visible
          </button>
          <button
            className="button button-secondary"
            onClick={downloadLogs}
            disabled={!lines.length}
          >
            <Icon name="download" />
            Download
          </button>
        </div>
      </div>
      {connectionMessage && (
        <div
          className={`notice ${connection === "error" ? "notice-warning" : "notice-info"}`}
          role={connection === "error" ? "alert" : "status"}
        >
          <Icon name={connection === "error" ? "alert" : "check"} />
          <span>{connectionMessage}</span>
        </div>
      )}
      {feedback && (
        <div className="notice notice-info" role="status">
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

      <section className="log-card" aria-label="Driver logs">
        <div className="log-toolbar">
          <div className="terminal-label">
            <Icon name="terminal" />
            <span>Driver logs</span>
            <span className="terminal-dot" />
          </div>
          <div className="log-search-controls">
            <label className="search-field log-search">
              <Icon name="search" />
              <span className="sr-only">Search logs</span>
              <input
                type="search"
                placeholder="Find in logs…"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
              />
            </label>
            <label className="wrap-control">
              <input
                type="checkbox"
                checked={wrap}
                onChange={(event) => setWrap(event.target.checked)}
              />
              Wrap lines
            </label>
            <button
              className="log-clear"
              onClick={clearLines}
              disabled={!lines.length}
            >
              Clear
            </button>
          </div>
        </div>
        <div
          ref={viewport}
          className={`log-viewport ${wrap ? "log-wrap" : ""}`}
          tabIndex={0}
          aria-label="Log output"
          onScroll={() => {
            const element = viewport.current;
            if (
              element &&
              follow &&
              element.scrollHeight - element.scrollTop - element.clientHeight >
                40
            )
              setFollow(false);
          }}
        >
          {visible.length ? (
            <div className="log-content">
              <pre className="log-numbers" aria-hidden="true">
                {visible.map((line) => line.number).join("\n")}
              </pre>
              <pre className="log-text" data-testid="log-text">
                {text}
              </pre>
            </div>
          ) : (
            <div className="log-empty">
              {query ? (
                <>
                  <Icon name="search" />
                  <strong>No matching log lines</strong>
                  <span>Try another search. New lines continue to arrive.</span>
                </>
              ) : (
                <>
                  <Icon name="terminal" />
                  <strong>
                    {connection === "connecting"
                      ? "Opening the log stream…"
                      : connection === "live"
                        ? "Waiting for log output"
                        : "No log lines to display"}
                  </strong>
                  <span>
                    {connection === "live"
                      ? "New lines will appear here as the driver writes them."
                      : "Use Reconnect to request available driver logs."}
                  </span>
                </>
              )}
            </div>
          )}
        </div>
        <div className="log-footer">
          <span>
            {visible.length.toLocaleString()} {query ? "matching" : "visible"} /{" "}
            {lines.length.toLocaleString()} buffered lines
            {dropped > 0
              ? ` · ${dropped.toLocaleString()} older lines discarded`
              : ""}
          </span>
          <span>
            {follow ? "Following output" : "Auto-scroll paused"} · Buffer limit{" "}
            {MAX_LOG_LINES.toLocaleString()}
          </span>
        </div>
      </section>
      <p className="log-help">
        Logs are displayed as plain text. Changing the tail limit or
        reconnecting starts a fresh buffer.
      </p>
    </>
  );
}
