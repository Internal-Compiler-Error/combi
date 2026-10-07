import { expect, test } from "vitest";
import { ancestorRole, kinship, prettyCountry } from "./names";

test("kinship", () => {
  expect(kinship(0, 1)).toBe("advisor and student");
  expect(kinship(3, 0)).toBe("academic great-grandparent and great-grandchild");
  expect(kinship(0, 5)).toBe("academic 3× great-grandparent and 3× great-grandchild");
  expect(kinship(1, 1)).toBe("academic siblings");
  expect(kinship(1, 2)).toBe("academic aunt or uncle and niece or nephew");
  expect(kinship(2, 2)).toBe("1st academic cousins");
  expect(kinship(3, 5)).toBe("2nd academic cousins twice removed");
  expect(kinship(12, 13)).toBe("11th academic cousins once removed");
  expect(kinship(23, 23)).toBe("22nd academic cousins");
});

test("ancestorRole", () => {
  expect(ancestorRole(1)).toBe("advisor");
  expect(ancestorRole(2)).toBe("academic grandparent");
  expect(ancestorRole(4)).toBe("academic 2× great-grandparent");
});

test("prettyCountry", () => {
  expect(prettyCountry("UnitedStates")).toBe("United States");
  expect(prettyCountry("HongKong")).toBe("Hong Kong");
});
