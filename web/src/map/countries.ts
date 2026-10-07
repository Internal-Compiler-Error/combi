// MGP names countries after its flag images ("UnitedStates"); the world map names its shapes
// in Natural Earth's short form ("United States of America", "Bosnia and Herz."). Most match once
// both are reduced to bare letters; these don't.
const SHAPE_FOR: Record<string, string> = {
  UnitedStates: "United States of America",
  CzechRepublic: "Czechia",
  BosniaHerzegovina: "Bosnia and Herz.",
  Catalonia: "Spain",
  England: "United Kingdom",
  Scotland: "United Kingdom",
  Wales: "United Kingdom",
  NorthernIreland: "United Kingdom",
  DominicanRepublic: "Dominican Rep.",
  CentralAfricanRepublic: "Central African Rep.",
  DemocraticRepublicOfCongo: "Dem. Rep. Congo",
  DemocraticRepublicOfTheCongo: "Dem. Rep. Congo",
  IvoryCoast: "Côte d'Ivoire",
  CoteDIvoire: "Côte d'Ivoire",
  SouthSudan: "S. Sudan",
  EquatorialGuinea: "Eq. Guinea",
  NorthMacedonia: "Macedonia",
  Swaziland: "eSwatini",
  Eswatini: "eSwatini",
  Burma: "Myanmar",
  TrinidadAndTobago: "Trinidad and Tobago",
  SolomonIslands: "Solomon Is.",
};

/** Places too small for the 1:110m map, drawn as a dot instead: [longitude, latitude]. */
export const DOTS: Record<string, [number, number]> = {
  Singapore: [103.82, 1.35],
  HongKong: [114.17, 22.32],
  Macau: [113.55, 22.17],
  Malta: [14.45, 35.9],
  Bahrain: [50.55, 26.07],
  Mauritius: [57.55, -20.25],
  Barbados: [-59.54, 13.19],
  Liechtenstein: [9.55, 47.16],
  Monaco: [7.42, 43.74],
  Andorra: [1.52, 42.51],
  SanMarino: [12.46, 43.94],
  VaticanCity: [12.45, 41.9],
  Grenada: [-61.68, 12.12],
  SaintLucia: [-60.98, 13.91],
};

export const letters = (s: string) =>
  s
    .normalize("NFD")
    .replace(/[^A-Za-z]/g, "")
    .toLowerCase();

/** Which map shape an MGP country is drawn as, comparable with `letters(shape name)`. */
export function shapeKey(mgpCountry: string): string {
  return letters(SHAPE_FOR[mgpCountry] ?? mgpCountry);
}
