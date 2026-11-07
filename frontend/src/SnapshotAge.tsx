import { useEffect, useState } from "react";

export function SnapshotAge({
  fetchedAt,
  label,
}: {
  fetchedAt: Date | null;
  label: string;
}) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);
  if (!fetchedAt) return <span>{label}: no successful fetch yet.</span>;
  const seconds = Math.max(0, Math.floor((now - fetchedAt.getTime()) / 1000));
  return (
    <span>
      {label} fetched{" "}
      <time
        dateTime={fetchedAt.toISOString()}
        title={fetchedAt.toLocaleString()}
        aria-label={`${fetchedAt.toLocaleString()} (${seconds}s ago)`}
      >
        {seconds}s ago
      </time>
      .
    </span>
  );
}
