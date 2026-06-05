SELECT *
FROM scrape_logs
WHERE page_scraped = $1
ORDER BY date DESC, id DESC
LIMIT 1