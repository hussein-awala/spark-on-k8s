import type { CSSProperties } from "react";

const paths = {
  grid: "M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z",
  search: "M21 21l-5-5 M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0",
  refresh:
    "M20 7v5h-5 M4 17v-5h5 M5.5 7a7 7 0 0 1 11.6-2L20 8 M4 16l2.9 3A7 7 0 0 0 18.5 17",
  arrow: "M5 12h14 M14 7l5 5-5 5",
  back: "M19 12H5 M10 7l-5 5 5 5",
  logs: "M5 3h14v18H5z M8 7h8 M8 11h8 M8 15h5",
  external:
    "M14 3h7v7 M21 3l-11 11 M11 3H4a1 1 0 0 0-1 1v16a1 1 0 0 0 1 1h16a1 1 0 0 0 1-1v-7",
  close: "M6 6l12 12 M18 6L6 18",
  download: "M12 3v12 M7 10l5 5 5-5 M4 16v5h16v-5",
  copy: "M8 8h13v13H8z M16 8V3H3v13h5",
  pause: "M8 5v14 M16 5v14",
  play: "M7 4l14 8-14 8z",
  check: "M5 12l4 4L19 6",
  alert: "M12 3L2 21h20L12 3z M12 9v5 M12 17h.01",
  trash: "M3 6h18 M9 6V3h6v3 M5 6l1 15h12l1-15 M10 10v7 M14 10v7",
  stop: "M6 6h12v12H6z",
  clock: "M12 8v4l3 2 M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0",
  layers: "M12 3L2 8l10 5 10-5-10-5z M2 12l10 5 10-5 M2 16l10 5 10-5",
  terminal: "M4 5h16v14H4z M7 9l3 3-3 3 M13 15h4",
} as const;

export function Icon({
  name,
  className,
  style,
}: {
  name: keyof typeof paths;
  className?: string;
  style?: CSSProperties;
}) {
  return (
    <svg
      aria-hidden="true"
      className={className}
      style={style}
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d={paths[name]} />
    </svg>
  );
}

export function SparkMark() {
  return (
    <svg
      aria-hidden="true"
      width="30"
      height="32"
      viewBox="0 0 30 32"
      fill="none"
    >
      <path d="m17 1-3 11H3l9 7-4 12 19-17H16l5-13h-4Z" fill="currentColor" />
    </svg>
  );
}
