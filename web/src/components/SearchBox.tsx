import { useEffect, useId, useRef, useState } from "react";
import { useNavigate } from "react-router";
import { degreeLine, useSearch } from "../api/client";

function useDebounced<T>(value: T, ms: number): T {
  const [v, setV] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setV(value), ms);
    return () => clearTimeout(t);
  }, [value, ms]);
  return v;
}

/**
 * Type-ahead over every mathematician in the database. Arrow keys move through suggestions,
 * Enter opens the highlighted one, or the full results page when nothing is highlighted.
 */
export function SearchBox({ className = "", autoFocus = false, large = false }: { className?: string; autoFocus?: boolean; large?: boolean }) {
  const [text, setText] = useState("");
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(-1);
  const q = useDebounced(text.trim(), 150);
  const { data: hits = [], isFetching } = useSearch(q);
  const navigate = useNavigate();
  const listId = useId();
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => setActive(-1), [q]);
  useEffect(() => {
    const close = (e: PointerEvent) => {
      if (!rootRef.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("pointerdown", close);
    return () => document.removeEventListener("pointerdown", close);
  }, []);

  const go = (path: string) => {
    setOpen(false);
    setText("");
    navigate(path);
  };

  const showList = open && text.trim().length > 0 && q.length > 0;

  return (
    <div ref={rootRef} className={`search ${large ? "search-large" : ""} ${className}`}>
      <input
        type="search"
        value={text}
        autoFocus={autoFocus}
        placeholder="Search by name, or MGP ID"
        aria-label="Search mathematicians"
        role="combobox"
        aria-expanded={showList}
        aria-controls={listId}
        aria-activedescendant={active >= 0 ? `${listId}-${active}` : undefined}
        onChange={(e) => {
          setText(e.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onKeyDown={(e) => {
          if (e.key === "ArrowDown" || e.key === "ArrowUp") {
            e.preventDefault();
            if (!hits.length) return;
            setOpen(true);
            // -1 is "nothing highlighted", so the cycle runs -1, 0, …, n-1, -1
            const n = hits.length;
            setActive((a) => (e.key === "ArrowDown" ? (a + 1 >= n ? -1 : a + 1) : a - 1 < -1 ? n - 1 : a - 1));
          } else if (e.key === "Enter") {
            const hit = hits[active];
            if (hit) go(`/m/${hit.id}`);
            else if (text.trim()) go(`/search?q=${encodeURIComponent(text.trim())}`);
          } else if (e.key === "Escape") {
            setOpen(false);
          }
        }}
      />
      {showList && (
        <ul id={listId} role="listbox" className="suggestions">
          {hits.map((p, i) => (
            <li key={p.id} id={`${listId}-${i}`} role="option" aria-selected={i === active}>
              <button type="button" onMouseEnter={() => setActive(i)} onClick={() => go(`/m/${p.id}`)}>
                <span className="s-name">{p.name}</span>
                <span className="s-meta">
                  {degreeLine(p)}
                  {p.student_count > 0 && ` · ${p.student_count} student${p.student_count === 1 ? "" : "s"}`}
                </span>
              </button>
            </li>
          ))}
          {!hits.length && !isFetching && <li className="s-empty">No one in the database matches “{q}”.</li>}
          {hits.length > 0 && (
            <li>
              <button type="button" className="s-all" onClick={() => go(`/search?q=${encodeURIComponent(text.trim())}`)}>
                See all results for “{text.trim()}”
              </button>
            </li>
          )}
        </ul>
      )}
    </div>
  );
}
