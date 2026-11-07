import { useState } from "react";
import { Dialog } from "./Dialog";

const MAX_EXCERPT_LINES = 1000;
const MAX_EXCERPT_CHARACTERS = 100_000;

export function downloadText(text: string, filename: string) {
  const url = URL.createObjectURL(
    new Blob([text], { type: "text/plain;charset=utf-8" }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

export function LogExcerpt({
  text,
  onClose,
}: {
  text: string;
  onClose: () => void;
}) {
  const initial = text
    .split("\n")
    .slice(0, MAX_EXCERPT_LINES)
    .join("\n")
    .slice(0, MAX_EXCERPT_CHARACTERS);
  const [preview, setPreview] = useState(initial);
  const [mask, setMask] = useState("");
  const [feedback, setFeedback] = useState("");
  function update(value: string) {
    setPreview(
      value
        .split("\n")
        .slice(0, MAX_EXCERPT_LINES)
        .join("\n")
        .slice(0, MAX_EXCERPT_CHARACTERS),
    );
    setFeedback("");
  }
  async function copy() {
    try {
      await navigator.clipboard.writeText(preview);
      setFeedback("Prepared excerpt copied.");
    } catch {
      setFeedback("Copy unavailable. Download the prepared excerpt instead.");
    }
  }
  return (
    <Dialog title="Prepare log excerpt" onClose={onClose}>
      <p>
        This frozen preview starts with the visible lines, including your
        search. Edit it to remove lines before sharing. Processing stays in this
        browser.
      </p>
      <p className="muted">
        Limited to the first 1,000 lines and 100,000 characters.
        {initial !== text && " The visible selection was truncated."} Masking is
        assistance only; review the entire preview for sensitive information.
      </p>
      <label className="excerpt-label">
        Excerpt preview
        <textarea
          aria-label="Excerpt preview"
          rows={12}
          maxLength={MAX_EXCERPT_CHARACTERS}
          value={preview}
          onChange={(event) => update(event.target.value)}
        />
      </label>
      <label className="excerpt-label">
        Exact value to mask
        <input
          value={mask}
          onChange={(event) => setMask(event.target.value)}
          autoComplete="off"
        />
      </label>
      <div className="dialog-actions">
        <button
          className="button button-secondary"
          disabled={!mask}
          onClick={() => {
            update(preview.split(mask).join("[MASKED]"));
            setMask("");
          }}
        >
          Mask exact value
        </button>
        <button
          className="button button-secondary"
          onClick={() =>
            update(
              preview
                .replace(
                  /(\b(?:password|token|secret|api[_-]?key)\s*[=:]\s*)([^\s,;]+)/gi,
                  "$1[MASKED]",
                )
                .replace(/(\bBearer\s+)[A-Za-z0-9._~+\/-]+/gi, "$1[MASKED]"),
            )
          }
        >
          Mask common credentials
        </button>
      </div>
      {feedback && <p role="status">{feedback}</p>}
      <div className="dialog-actions">
        <button
          className="button button-secondary"
          disabled={!preview}
          onClick={() => void copy()}
        >
          Copy prepared excerpt
        </button>
        <button
          className="button button-secondary"
          disabled={!preview}
          onClick={() => downloadText(preview, "spark-log-excerpt.txt")}
        >
          Download prepared excerpt
        </button>
      </div>
    </Dialog>
  );
}
