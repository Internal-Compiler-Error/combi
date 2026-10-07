import { Link } from "react-router";

export function NotFoundPage() {
  return (
    <main className="page narrow">
      <h1 className="page-title">Page not found</h1>
      <p className="muted">
        Try searching above, or go back to the <Link to="/">start page</Link>.
      </p>
    </main>
  );
}
