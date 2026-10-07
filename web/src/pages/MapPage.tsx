import { useEffect, useMemo } from "react";
import { useSearchParams } from "react-router";
import { useCountries, useCountrySchools, type CountryStat } from "../api/client";
import { WorldMap } from "../map/WorldMap";
import { breaks, regionsFor, useWorld, type Region } from "../map/world";
import { CountUp, rise } from "../motion";

const fmt = new Intl.NumberFormat();

/** Where mathematicians in the database graduated: a world map plus the same numbers as lists. */
export default function MapPage() {
  const countries = useCountries();
  const world = useWorld();
  const [params, setParams] = useSearchParams();
  // the selection lives in the URL (?c=Germany, MGP's name) so it can be shared and survives reloads
  const selected = params.get("c");
  useEffect(() => void (document.title = "Map · Combi"), []);

  const placed = useMemo(
    () => (countries.data && world.data ? regionsFor(countries.data, world.data) : null),
    [countries.data, world.data],
  );
  const thresholds = useMemo(() => breaks(Math.max(1, ...[...(placed?.regions.values() ?? [])].map((r) => r.mathematicians))), [placed]);
  const selectedRegion = selected && placed ? [...placed.regions.values()].find((r) => r.parts.some((p) => p.country === selected)) : undefined;
  const selectedCountry = countries.data?.find((c) => c.country === selected) ?? null;

  const select = (country: string | null) => setParams(country ? { c: country } : {});

  useEffect(() => {
    const clear = (e: KeyboardEvent) => e.key === "Escape" && setParams({});
    window.addEventListener("keydown", clear);
    return () => window.removeEventListener("keydown", clear);
  }, [setParams]);

  const error = countries.error ?? world.error;
  return (
    <main className="mappage">
      <header>
        <h1 className="page-title">Where they graduated</h1>
        <p className="muted">
          Mathematicians in the database by the country of the school that awarded their degree. Hover a country for its numbers;
          click one for its schools.
        </p>
      </header>
      {error && <p className="error">{error.message}</p>}
      <div className="map-layout">
        <section className="map-card">
          {placed && world.data ? (
            <WorldMap
              shapes={world.data}
              regions={placed.regions}
              thresholds={thresholds}
              selected={selectedRegion?.key ?? null}
              onSelect={(r: Region | null) => select(r?.parts[0]!.country ?? null)}
            />
          ) : (
            !error && <div className="map map-loading" aria-busy="true" />
          )}
        </section>
        <aside className="map-side">
          {selectedCountry ? (
            <CountryDetail country={selectedCountry} onBack={() => select(null)} />
          ) : (
            countries.data && <CountryList countries={countries.data} unplaced={placed?.unplaced ?? []} onSelect={select} />
          )}
        </aside>
      </div>
    </main>
  );
}

function CountryList({ countries, unplaced, onSelect }: { countries: CountryStat[]; unplaced: CountryStat[]; onSelect: (c: string) => void }) {
  const max = Math.max(1, ...countries.map((c) => c.mathematicians));
  const total = countries.reduce((n, c) => n + c.mathematicians, 0);
  if (!countries.length) return <p className="muted">No one in the database has a school with a known country yet.</p>;
  return (
    <>
      <p className="label">
        <CountUp value={total} /> mathematicians · {countries.length} countries
      </p>
      <ol className="bars">
        {countries.map((c, i) => (
          <li key={c.country} className="rise" style={rise(i)}>
            <button type="button" onClick={() => onSelect(c.country)}>
              <span className="bar-name">
                {c.name}
                {unplaced.includes(c) && <span className="muted small"> · not on the map</span>}
              </span>
              <span className="bar-value mono">{fmt.format(c.mathematicians)}</span>
              <span className="bar" style={{ "--w": c.mathematicians / max, "--i": Math.min(i, 12) } as React.CSSProperties} />
            </button>
          </li>
        ))}
      </ol>
    </>
  );
}

function CountryDetail({ country, onBack }: { country: CountryStat; onBack: () => void }) {
  const schools = useCountrySchools(country.country);
  const max = Math.max(1, ...(schools.data ?? []).map((s) => s.mathematicians));
  return (
    <div className="country" key={country.country}>
      <button type="button" className="back" onClick={onBack}>
        ← All countries
      </button>
      <h2 className="country-name">{country.name}</h2>
      <dl className="counts">
        <div>
          <dt>Mathematicians</dt>
          <dd>
            <CountUp value={country.mathematicians} />
          </dd>
        </div>
        <div>
          <dt>Schools</dt>
          <dd>
            <CountUp value={country.schools} />
          </dd>
        </div>
      </dl>
      {schools.isPending && <p className="muted small">Loading schools…</p>}
      {schools.isError && <p className="error">{schools.error.message}</p>}
      {schools.data && (
        <ol className="bars">
          {schools.data.map((s, i) => (
            <li key={s.school} className="rise" style={rise(i)}>
              <div className="bar-row">
                <span className="bar-name">{s.school}</span>
                <span className="bar-value mono">{fmt.format(s.mathematicians)}</span>
                <span className="bar" style={{ "--w": s.mathematicians / max, "--i": Math.min(i, 12) } as React.CSSProperties} />
              </div>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
