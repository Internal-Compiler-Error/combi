import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useCrawl, useWalk, type WalkStatus } from "../api/client";

const dateFormat = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" });
const fmt = new Intl.NumberFormat();

type Props = {
  id: number;
  lastCrawled: string | null;
  nextCrawl: string | null;
  /** a walk already under way from this person, so a reload picks its progress back up */
  walk?: WalkStatus | null;
  label: string;
  compact?: boolean;
};

/**
 * Crawls someone's MGP page and walks their tree in the background: their students' pages, theirs,
 * and so on, and their advisors' up the line. Pages that aren't due for a recrawl are followed
 * through the database without fetching, so walking again soon after is cheap.
 */
export function CrawlButton({ id, lastCrawled, nextCrawl, walk: existing = null, label, compact = false }: Props) {
  const crawl = useCrawl(id);
  const walk = useWalk(crawl.data?.walk ?? existing);
  const w = walk.data;
  const running = w?.status === "running";

  // a finished walk has added people and links all over: refresh what's on screen, once
  const client = useQueryClient();
  const wasRunning = useRef(running);
  useEffect(() => {
    if (wasRunning.current && !running) void client.invalidateQueries({ predicate: (q) => q.queryKey[0] !== "walk" });
    wasRunning.current = running;
  }, [running, client]);

  const due = nextCrawl ? Date.parse(nextCrawl) : 0;
  const status = lastCrawled
    ? `Crawled ${dateFormat.format(new Date(lastCrawled))} · ${due > Date.now() ? `next check ${dateFormat.format(new Date(due))}` : "due for a check"}`
    : null;
  const done = w ? w.fetched + w.skipped + w.failed : 0;
  const summary = w && `${fmt.format(w.fetched)} fetched · ${fmt.format(w.skipped)} already up to date${w.failed ? ` · ${fmt.format(w.failed)} not on MGP` : ""}`;

  return (
    <div className={compact ? "crawl crawl-compact" : "crawl"}>
      <button
        type="button"
        className="btn"
        onClick={() => crawl.mutate()}
        disabled={crawl.isPending || running}
        aria-busy={crawl.isPending || running}
        title={compact ? [status, summary].filter(Boolean).join("\n") || undefined : undefined}
      >
        {crawl.isPending ? "Crawling…" : running ? (compact ? `${fmt.format(done)}…` : "Walking their tree…") : label}
      </button>
      {!compact && status && <span className="muted small">{status}</span>}
      {!compact && w && (
        <div className="walk" role="status">
          {running && (
            <span className="walk-bar" aria-hidden="true">
              <span style={{ width: `${(100 * done) / Math.max(1, done + w.todo)}%` }} />
            </span>
          )}
          <span className="muted small">
            {running
              ? `Walking their tree: ${summary} · ${fmt.format(w.todo)} to go`
              : w.status === "capped"
                ? `Walk stopped after ${fmt.format(w.fetched)} pages: ${summary}. Crawl again later to go further.`
                : `Walked their tree: ${summary}.`}
          </span>
        </div>
      )}
      {crawl.isError && (
        <span className="error small" role="alert">
          {crawl.error.message}
        </span>
      )}
    </div>
  );
}
