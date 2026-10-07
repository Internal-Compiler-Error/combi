import { Link, NavLink, Outlet, useLocation } from "react-router";
import { SearchBox } from "./SearchBox";

export function Layout() {
  const { pathname } = useLocation();
  const onHome = pathname === "/";
  return (
    <div className="shell">
      <header className="topbar">
        <Link to="/" className="wordmark">
          <span className="wordmark-name">Combi</span>
          <span className="wordmark-sub">Mathematics genealogy</span>
        </Link>
        <nav className="topnav">
          <NavLink to="/map">Map</NavLink>
          <NavLink to="/flows">Flows</NavLink>
          <NavLink to="/relate">Relate</NavLink>
        </nav>
        {!onHome && <SearchBox className="topbar-search" />}
      </header>
      {/* keyed by path so each page eases in; staying on a page (new search, new depth) doesn't replay it */}
      <div className="route" key={pathname}>
        <Outlet />
      </div>
    </div>
  );
}
