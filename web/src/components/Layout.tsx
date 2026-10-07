import { Link, Outlet, useLocation } from "react-router";
import { SearchBox } from "./SearchBox";

export function Layout() {
  const onHome = useLocation().pathname === "/";
  return (
    <div className="shell">
      <header className="topbar">
        <Link to="/" className="wordmark">
          <span className="wordmark-name">Combi</span>
          <span className="wordmark-sub">Mathematics genealogy</span>
        </Link>
        {!onHome && <SearchBox className="topbar-search" />}
      </header>
      <Outlet />
    </div>
  );
}
