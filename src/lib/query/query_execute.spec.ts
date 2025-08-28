import { describe, test, expect } from "vitest";
import { Context } from "./context";
import { AttributeColumn } from "./nodes";
import { Table } from "./models";
import { INTEGER, normalizeColumns } from "./types";

describe("Simple Queries", () => {
  const context = new Context().withDefaultTable("postings").withTables(
    Table.create(
      "postings",
      new AttributeColumn("a", Number),
      new AttributeColumn("b", String),
    ).data([
      { a: 1, b: "one" },
      { a: 2.03, b: "two" },
    ]),
  );

  test("Execute multiple", () => {
    const [columns, data] = context.execute(`
      CREATE TABLE t1(a STRING, b INTEGER);
      
      INSERT INTO t1 
      VALUES('peter',1), ('pan', 2);

      SELECT * from t1;
    `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "a", type: String },
      { name: "b", type: INTEGER },
    ]);
    expect(data).toEqual([["peter", 1], ["pan", 2]]);
  });


  test("SELECT sum(a)", () => {
    const [columns, data] = context.execute(`SELECT sum(a).toFixed(1) total`);
    expect(normalizeColumns(columns)).toEqual([
      { name: "total", type: String },
    ]);
    expect(data).toEqual([["3.0"]]);
  });

  test("SELECT a.toFixed(3)", () => {
    const [columns, data] = context.execute(`SELECT a.toFixed(3)`);
    expect(normalizeColumns(columns)).toEqual([
      { name: "a.toFixed(3)", type: String },
    ]);
    expect(data).toEqual([["1.000"], ["2.030"]]);
  });

  test("SELECT a.toFixed(3)::text as str", () => {
    const [columns, data] = context.execute(`SELECT a.toFixed(3)::text as str`);
    expect(normalizeColumns(columns)).toEqual([{ name: "str", type: String }]);
    expect(data).toEqual([["1.000"], ["2.030"]]);
  });

  [
    "SELECT * FROM postings",
    "SELECT postings.* FROM postings",
    "SELECT p.* FROM postings p",
  ].forEach((query) => {
    test("SELECT all wildcard columns: " + query, () => {
      const [columns, data] = context.execute(query);
      expect(normalizeColumns(columns)).toEqual([
        { name: "a", type: Number },
        { name: "b", type: String },
      ]);
      expect(data).toEqual([
        [1, "one"],
        [2.03, "two"],
      ]);
    });
  });

  test("SELECT duplicate columns with *", () => {
    const [columns, data] = context.execute(`SELECT *, * FROM postings`);
    expect(normalizeColumns(columns)).toEqual([
      { name: "a", type: Number },
      { name: "b", type: String },
      { name: "a", type: Number },
      { name: "b", type: String },
    ]);
    expect(data).toEqual([
      [1, "one", 1, "one"],
      [2.03, "two", 2.03, "two"],
    ]);
  });

  test("SELECT wildcard with additional column", () => {
    const [columns, data] = context.execute(`SELECT -a, * FROM postings`);
    expect(normalizeColumns(columns)).toEqual([
      { name: "-(a)", type: Number },
      { name: "a", type: Number },
      { name: "b", type: String },
    ]);
    expect(data).toEqual([
      [-1, 1, "one"],
      [-2.03, 2.03, "two"],
    ]);
  });
});
