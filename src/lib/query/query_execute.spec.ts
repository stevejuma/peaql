import { describe, test, expect } from "vitest";
import { Context } from "./context";
import { AttributeColumn } from "./nodes";
import { Table } from "./models";
import { INTEGER, normalizeColumns } from "./types";
import { DateTime } from "luxon";

describe("Update table", () => {
  test("Update single row", () => {
    const context = Context.create();
    const updated = context.execute(`
        CREATE TABLE genre
        (
            genre_id INT NOT NULL,
            name VARCHAR(120),
            CONSTRAINT genre_pkey PRIMARY KEY  (genre_id)
        );
        INSERT INTO genre (genre_id, name) VALUES
        (1, 'Rock'),
        (2, 'Jazz');
        UPDATE genre SET name = 'Rock & Roll'
        WHERE genre_id = 1;
      `);

    expect(updated).toBe(1);
  });

  test("Update returning", () => {
    const context = Context.create();
    const [columns, data] = context.execute(`
        CREATE TABLE genre
        (
            genre_id INT NOT NULL,
            name VARCHAR(120),
            CONSTRAINT genre_pkey PRIMARY KEY  (genre_id)
        );
        INSERT INTO genre (genre_id, name) VALUES
        (1, 'Rock'),
        (2, 'Jazz');
        UPDATE genre SET name = 'Rock & Roll'
        WHERE genre_id = 1
        RETURNING name, length(name)
      `);

    expect(normalizeColumns(columns)).toEqual([
      { name: "name", type: String },
      { name: "length(name)", type: Number },
    ]);

    expect(data).toEqual([["Rock & Roll", 11]]);
  });
});

describe("Create table", () => {
  test("Create timestamp", () => {
    const context = Context.create();
    const [columns, data] = context.execute(`
          CREATE TABLE timezones
          (
              id INT NOT NULL,
              created_at TIMESTAMP
          );
          INSERT INTO timezones (id, created_at) VALUES
          (1, '2025-12-12')
          RETURNING *;
        `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "id", type: INTEGER },
      { name: "created_at.date", type: DateTime },
      { name: "created_at.year", type: Number },
      { name: "created_at.month", type: Number },
      { name: "created_at.day", type: Number },
    ]);
    expect(data).toEqual([
      [1, DateTime.fromISO("2025-12-12T00:00:00.000+00:00"), 2025, 12, 12],
    ]);
  });

  test("Fails on invalid default value", () => {
    const context = Context.create();
    expect(() => {
      context.execute(`
          CREATE TABLE genre
          (
              genre_id INT NOT NULL,
              name VARCHAR(120) DEFAULT true,
              CONSTRAINT genre_pkey PRIMARY KEY  (genre_id)
          );
          INSERT INTO genre (genre_id, name) VALUES
          (1, 'Rock'),
          (2, 'Jazz');
        `);
    }).toThrow(`Invalid type boolean for string column "genre"."name"`);
  });

  test("Inserts value with default", () => {
    const context = Context.create();
    const [columns, data] = context.execute(`
      CREATE TABLE genre
      (
          genre_id INT NOT NULL,
          name VARCHAR(120) DEFAULT 'Pop'
      );
      INSERT INTO genre (genre_id) VALUES
      (1)
    `);

    expect(normalizeColumns(columns)).toEqual([
      { name: "genre_id", type: INTEGER },
      { name: "name", type: String },
    ]);
    expect(data).toEqual([[1, "Pop"]]);
  });

  test("Inserts value with default returning *", () => {
    const context = Context.create();
    const [columns, data] = context.execute(`
      CREATE TABLE genre
      (
          genre_id INT NOT NULL,
          name VARCHAR(120) DEFAULT 'Pop'
      );
      INSERT INTO genre (genre_id) VALUES
      (1)
      RETURNING *
    `);

    expect(normalizeColumns(columns)).toEqual([
      { name: "genre_id", type: INTEGER },
      { name: "name", type: String },
    ]);
    expect(data).toEqual([[1, "Pop"]]);
  });

  test("Inserts with returning", () => {
    const context = Context.create();
    const [columns, data] = context.execute(`
      CREATE TABLE genre
      (
          genre_id INT NOT NULL,
          name VARCHAR(120) DEFAULT 'Pop'
      );
      INSERT INTO genre (genre_id, name) VALUES
      (1, 'Rock')
      RETURNING name
    `);

    expect(normalizeColumns(columns)).toEqual([{ name: "name", type: String }]);
    expect(data).toEqual([["Rock"]]);
  });

  test("Creates timestamp", () => {
    const context = Context.create();
    context.execute(`
        CREATE TABLE t1(a timestamp); 
        INSERT INTO t1(a) VALUES('2022-07-17');
      `);
  });

  test("Fails when creating table that exists", () => {
    const context = Context.create();
    expect(() => {
      context.execute(`
        CREATE TABLE t1(a STRING, b INTEGER);
        CREATE TABLE t1(a STRING, b INTEGER);
      `);
    }).toThrow('relation "t1" already exists');
  });

  test("Does not create a table if it exists", () => {
    const context = Context.create();
    context.execute(`
        CREATE TABLE t1(a STRING, b INTEGER);
        CREATE TABLE IF NOT EXISTS t1(a STRING, b INTEGER);
      `);
  });

  test("Fails when inserting wrong type", () => {
    const context = Context.create();
    expect(() => {
      context.execute(`
          CREATE TABLE t1(a STRING, b INTEGER);
          INSERT INTO t1(a,b) VALUES(55, 'a');
      `);
    }).toThrow(`invalid input syntax for type string: 55`);
  });

  test("Evaluates table constraints on insert", () => {
    const context = Context.create();
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
    const context = Context.create();
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
    const context = Context.create();
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

describe("Column Identifiers", () => {
  const context = Context.create()
    .withDefaultTable("postings")
    .withTables(
      Table.create(
        "postings",
        new AttributeColumn("a", Number),
        new AttributeColumn("b", String),
      ).data([{ a: 1, b: "one" }]),
    );

  test("Select quoted identifiers", () => {
    const [columns, data] = context.execute(`
        SELECT a, [a] as bracket, "a" as quoted, \`a\` as backtick from postings;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "a", type: Number },
      { name: "bracket", type: Number },
      { name: "quoted", type: Number },
      { name: "backtick", type: Number },
    ]);
    expect(data).toEqual([[1, 1, 1, 1]]);
  });

  test("identifier_quoting = bracket", () => {
    const [columns, data] = context.execute(`
        SET identifier_quoting = bracket;
        SELECT a, [a] as bracket, "a" as quoted, \`a\` as backtick from postings;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "a", type: Number },
      { name: "bracket", type: Number },
      { name: "quoted", type: String },
      { name: "backtick", type: String },
    ]);
    expect(data).toEqual([[1, 1, "a", "a"]]);
  });

  test("identifier_quoting = quoted", () => {
    const [columns, data] = context.execute(`
        SET identifier_quoting = quoted;
        SELECT a, [a] as bracket, "a" as quoted, \`a\` as backtick from postings;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "a", type: Number },
      { name: "bracket", type: String },
      { name: "quoted", type: Number },
      { name: "backtick", type: String },
    ]);
    expect(data).toEqual([[1, "a", 1, "a"]]);
  });

  test("identifier_quoting = backtick", () => {
    const [columns, data] = context.execute(`
        SET identifier_quoting = backtick;
        SELECT a, [a] as bracket, "a" as quoted, \`a\` as backtick from postings;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "a", type: Number },
      { name: "bracket", type: String },
      { name: "quoted", type: String },
      { name: "backtick", type: Number },
    ]);
    expect(data).toEqual([[1, "a", "a", 1]]);
  });

  test("invalid option identifier_quoting", () => {
    expect(() => {
      context.execute(`
          SET identifier_quoting = unknown;
          SELECT a, [a] as bracket, "a" as quoted, \`a\` as backtick from postings;
        `);
    }).toThrow(
      "Invalid value for option: identifier_quoting expected: quoted,backtick,bracket,auto got unknown",
    );
  });
});

describe("Simple Queries", () => {
  const context = Context.create()
    .withDefaultTable("postings")
    .withTables(
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
    const context = Context.create();
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

  test("SELECT (a).toFixed(3)", () => {
    const [columns, data] = context.execute(`SELECT (a).toFixed(3)`);
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

describe("Boolean Queries", () => {
  const context = Context.create();
  context.execute(`
    CREATE TABLE t1
    (
        id INT NOT NULL,
        name TEXT,
        active BOOLEAN
    );
    INSERT INTO t1 (id, name, active) VALUES
    (1, 'peter', true),
    (1, 'pan', false);
  `);

  test("WHERE boolean", () => {
    const [columns, data] = context.execute(`
        SELECT name FROM t1 where active
    `);
    expect(normalizeColumns(columns)).toEqual([{ name: "name", type: String }]);
    expect(data).toEqual([["peter"]]);
  });

  test("WHERE !boolean", () => {
    const [columns, data] = context.execute(`
        SELECT name FROM t1 where !active
    `);
    expect(normalizeColumns(columns)).toEqual([{ name: "name", type: String }]);
    expect(data).toEqual([["pan"]]);
  });

  test("concat", () => {
    const [, data] = context.execute(`
        SELECT name || id from t1
    `);

    expect(data).toEqual([["peter1"], ["pan1"]]);
  });
});
