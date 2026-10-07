import { RECRAWL_AFTER_DAYS, useCrawl } from "../api/client";

const dateFormat = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" });

const DAY = 86_400_000;

/** Fetches someone's MGP page into the database. Disabled while the last crawl is under two weeks
 * old, matching the API, which won't fetch such a page again. */
export function CrawlButton({ id, lastCrawled, label, compact = false }: { id: number; lastCrawled: string | null; label: string; compact?: boolean }) {
  const crawl = useCrawl(id);
  const nextCrawl = lastCrawled ? Date.parse(lastCrawled) + RECRAWL_AFTER_DAYS * DAY : 0;
  const fresh = nextCrawl > Date.now();
  const status = lastCrawled
    ? `Crawled ${dateFormat.format(new Date(lastCrawled))}${fresh ? ` · can crawl again after ${dateFormat.format(new Date(nextCrawl))}` : ""}`
    : null;
  return (
    <div className={compact ? "crawl crawl-compact" : "crawl"}>
      <button type="button" className="btn" onClick={() => crawl.mutate()} disabled={fresh || crawl.isPending} title={compact ? (status ?? undefined) : undefined}>
        {crawl.isPending ? "Crawling…" : label}
      </button>
      {status && !compact && <span className="muted small">{status}</span>}
      {crawl.isError && (
        <span className="error small" role="alert">
          {crawl.error.message}
        </span>
      )}
    </div>
  );
}
