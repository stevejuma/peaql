import { describe, test, expect } from "vitest";
import { Context } from "./context";
import { AttributeColumn } from "./nodes";
import { Table } from "./models";
import { normalizeColumns } from "./types";
import { Decimal } from "../decimal";

describe("Simple Queries", () => {
  const context = new Context().withTables(
    Table.create(
      "postings",
      new AttributeColumn("a", Number),
      new AttributeColumn("b", String),
    ).data([
      { a: 1, b: "one" },
      { a: 2.03, b: "two" },
    ]),
  );
  test("SELECT a.toFixed(3)", () => {
    const [columns, data] = context.execute(`SELECT a.toFixed(3)`);
    expect(normalizeColumns(columns)).toEqual([
      { name: "a.toFixed(3)", type: Decimal },
    ]);
    expect(data).toEqual([[new Decimal("1.000")], [new Decimal("2.030")]]);
  });

  test("SELECT a.toFixed(3)::text as str", () => {
    const [columns, data] = context.execute(`SELECT a.toFixed(3)::text as str`);
    expect(normalizeColumns(columns)).toEqual([{ name: "str", type: String }]);
    expect(data).toEqual([["1.000"], ["2.030"]]);
  });
});
