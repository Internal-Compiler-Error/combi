import { Link } from "react-router";
import type { Person } from "../api/client";

/** "Ph.D. 1963 · California Institute of Technology", with the school linking to its page. */
export function DegreeLine({ p }: { p: Pick<Person, "year" | "school" | "school_id"> }) {
  return (
    <>
      {p.year ? `Ph.D. ${p.year}` : "Year unknown"}
      {p.school && (
        <>
          {" · "}
          {p.school_id !== null ? <Link to={`/s/${p.school_id}`}>{p.school}</Link> : p.school}
        </>
      )}
    </>
  );
}
