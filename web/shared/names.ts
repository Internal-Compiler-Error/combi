// Wording shared by the API and the website.

/** The Mathematics Genealogy Project names countries after its flag images ("UnitedStates"); put the spaces back. */
export function prettyCountry(raw: string): string {
  return raw.replace(/(\p{Ll})(\p{Lu})/gu, "$1 $2");
}

const ordinal = (n: number) => `${n}${n % 100 >= 11 && n % 100 <= 13 ? "th" : ["th", "st", "nd", "rd"][n % 10] ?? "th"}`;
const greats = (n: number) => (n <= 0 ? "" : n === 1 ? "great-" : `${n}× great-`);
const removed = (n: number) => (n === 1 ? "once removed" : n === 2 ? "twice removed" : `${n} times removed`);

/** What someone `generations` above a person is to them: "advisor", "academic grandparent", … */
export function ancestorRole(generations: number): string {
  return generations === 1 ? "advisor" : `academic ${greats(generations - 2)}grandparent`;
}

/**
 * What two people are to each other, given how many generations each is below their nearest
 * shared academic ancestor — the family-tree words, with advisors as parents.
 */
export function kinship(a: number, b: number): string {
  if (a === 0 && b === 0) return "the same person";
  if (a === 0 || b === 0) {
    const g = a + b;
    return g === 1 ? "advisor and student" : `academic ${greats(g - 2)}grandparent and ${greats(g - 2)}grandchild`;
  }
  const cousin = Math.min(a, b) - 1;
  const apart = Math.abs(a - b);
  if (cousin === 0) return apart === 0 ? "academic siblings" : `academic ${greats(apart - 1)}aunt or uncle and ${greats(apart - 1)}niece or nephew`;
  return `${ordinal(cousin)} academic cousins${apart ? ` ${removed(apart)}` : ""}`;
}
