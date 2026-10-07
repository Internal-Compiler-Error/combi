import { RECRAWL_AFTER_DAYS, useCrawl } from "../api/client";

const dateFormat = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" });

/** Whether a page crawled at `lastCrawled` may be fetched from MGP again. */
export const crawlable = (lastCrawled: string | null) =>
  !lastCrawled || Date.now() - Date.parse(lastCrawled) >= RECRAWL_AFTER_DAYS * 86_400_000;

/** Fetches someone's MGP page into the database; the API refuses pages crawled in the last two weeks. */
export function CrawlButton({ id, lastCrawled, label }: { id: number; lastCrawled: string | null; label: string }) {
  const crawl = useCrawl(id);
  return (
    <div className="crawl">
      {crawlable(lastCrawled) ? (
        <button type="button" className="btn" onClick={() => crawl.mutate()} disabled={crawl.isPending}>
          {crawl.isPending ? "Crawling…" : label}
        </button>
      ) : null}
      {lastCrawled && <span className="muted small">Crawled {dateFormat.format(new Date(lastCrawled))}</span>}
      {crawl.isError && (
        <span className="error small" role="alert">
          {crawl.error.message}
        </span>
      )}
    </div>
  );
}
