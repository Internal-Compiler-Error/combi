import { RECRAWL_AFTER_DAYS, useCrawl } from "../api/client";

const dateFormat = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" });

const DAY = 86_400_000;

/** Fetches someone's MGP page into the database. Disabled while the last crawl is under two weeks
 * old, matching the API, which won't fetch such a page again. */
export function CrawlButton({ id, lastCrawled, label }: { id: number; lastCrawled: string | null; label: string }) {
  const crawl = useCrawl(id);
  const nextCrawl = lastCrawled ? Date.parse(lastCrawled) + RECRAWL_AFTER_DAYS * DAY : 0;
  const fresh = nextCrawl > Date.now();
  return (
    <div className="crawl">
      <button type="button" className="btn" onClick={() => crawl.mutate()} disabled={fresh || crawl.isPending}>
        {crawl.isPending ? "Crawling…" : label}
      </button>
      {lastCrawled && (
        <span className="muted small">
          Crawled {dateFormat.format(new Date(lastCrawled))}
          {fresh && ` · can crawl again after ${dateFormat.format(new Date(nextCrawl))}`}
        </span>
      )}
      {crawl.isError && (
        <span className="error small" role="alert">
          {crawl.error.message}
        </span>
      )}
    </div>
  );
}
