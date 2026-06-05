SELECT *
FROM scrape_logs
WHERE page_scraped = ?
ORDER BY date DESC, id DESC
LIMIT 1