import { describe, test, expect } from "vitest";
import { Context } from "./context";
import { AttributeColumn } from "./nodes";
import { Table } from "./models";
import { INTEGER, normalizeColumns } from "./types";

describe("Create table", () => {
  test("Creates timestamp", () => {
    const context = new Context();
    context.execute(`
        CREATE TABLE t1(a timestamp); 
        INSERT INTO t1(a) VALUES('2022-07-17');
      `);
  });

  test("Fails when creating table that exists", () => {
    const context = new Context();
    expect(() => {
      context.execute(`
        CREATE TABLE t1(a STRING, b INTEGER);
        CREATE TABLE t1(a STRING, b INTEGER);
      `);
    }).toThrow('relation "t1" already exists');
  });

  test("Does not create a table if it exists", () => {
    const context = new Context();
    context.execute(`
        CREATE TABLE t1(a STRING, b INTEGER);
        CREATE TABLE IF NOT EXISTS t1(a STRING, b INTEGER);
      `);
  });

  test("Fails when inserting wrong type", () => {
    const context = new Context();
    expect(() => {
      context.execute(`
          CREATE TABLE t1(a STRING, b INTEGER);
          INSERT INTO t1(a,b) VALUES(55, 'a');
      `);
    }).toThrow(`invalid input syntax for type string: 55`);
  });

  test("Evaluates table constraints on insert", () => {
    const context = new Context();
    expect(() => {
      context.execute(`
          CREATE TABLE t1(a STRING, b INTEGER, CHECK(b > 100));
          INSERT INTO t1(a,b) VALUES('a', 55);
      `);
    }).toThrow(
      `Failing row contains (a, 55). new row for relation "t1" violates check constraint "t1_b_check"`,
    );
  });

  test("Evaluates column constraints on insert", () => {
    const context = new Context();
    expect(() => {
      context.execute(`
          CREATE TABLE t1(a STRING CHECK(b > 100), b INTEGER);
          INSERT INTO t1(a,b) VALUES('a', 55);
      `);
    }).toThrow(
      `Failing row contains (a, 55). new row for relation "t1" violates check constraint "t1_a_check"`,
    );
  });

  test("Enforces not null constraint", () => {
    const context = new Context();
    expect(() => {
      context.execute(`
          CREATE TABLE t1(a STRING, b INTEGER NOT NULL);
          INSERT INTO t1(a,b) VALUES('a', null);
      `);
    }).toThrow(
      `Failing row contains (a, null). null value in column "b" of relation "t1" violates not-null constraint`,
    );
  });
});

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
    expect(data).toEqual([
      ["peter", 1],
      ["pan", 2],
    ]);
  });

  test("SELECT date structure", () => {
    const context = new Context();
    const [columns, data] = context.execute(
      `SELECT '2022-12-12'::timestamp.month as date`,
    );
    expect(normalizeColumns(columns)).toEqual([{ name: "date", type: Number }]);
    expect(data).toEqual([[12]]);
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
