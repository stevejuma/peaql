import { describe, test, expect } from "vitest";
import { Context } from "./context";
import { AttributeColumn } from "./nodes";
import { Table } from "./models";
import { DType, normalizeColumns } from "./types";

type TestCase = {
  input: string | string[];
  columns?: Array<{ name: string; type: DType }>;
  data?: unknown[][] | unknown | ((input: TestCase) => void);
};

const context = new Context();
context.tables.set(
  "t1",
  Table.create(
    "t1",
    new AttributeColumn("a", Number),
    new AttributeColumn("b", String),
    new AttributeColumn("c", String),
  ).data([
    { a: 1, b: "A", c: "one" },
    { a: 2, b: "B", c: "two" },
    { a: 3, b: "C", c: "three" },
    { a: 4, b: "D", c: "one" },
    { a: 5, b: "E", c: "two" },
    { a: 6, b: "F", c: "three" },
    { a: 7, b: "G", c: "one" },
  ]),
);

context.tables.set(
  "t2",
  Table.create(
    "t2",
    new AttributeColumn("a", Number),
    new AttributeColumn("b", String),
  ).data([
    { a: "a", b: "one" },
    { a: "a", b: "two" },
    { a: "a", b: "three" },
    { a: "b", b: "four" },
    { a: "c", b: "five" },
    { a: "c", b: "six" },
  ]),
);

context.tables.set(
  "sales",
  Table.create(
    "sales",
    new AttributeColumn("region", String),
    new AttributeColumn("product", String),
    new AttributeColumn("revenue", Number),
  ).data([
    { region: "North", product: "A", revenue: 100 },
    { region: "North", product: "B", revenue: 200 },
    { region: "South", product: "A", revenue: 150 },
    { region: "South", product: "C", revenue: 300 },
    { region: "East", product: "B", revenue: 250 },
    { region: "East", product: "C", revenue: 100 },
    { region: "West", product: "A", revenue: 50 },
    { region: "West", product: "B", revenue: 300 },
  ]),
);

const testCases: Record<string, Array<TestCase>> = {
  "Group By": [
    {
      input: `
       SELECT
          product,
          SUM(revenue) AS product_revenue,
          SUM(SUM(revenue)) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS product_total
        FROM
          sales
        GROUP BY
          product
        order by product
      `,
      columns: [
        { name: "product", type: String },
        { name: "product_revenue", type: Number },
        { name: "product_total", type: Number },
      ],
      data: [
        ["A", 300, 300],
        ["B", 750, 1050],
        ["C", 400, 1450],
      ],
    },
    {
      input: `
        SELECT
          product,
          SUM(revenue) AS product_revenue,
          SUM(revenue) * 1.0 / SUM(SUM(revenue)) OVER() AS revenue_percentage
        FROM
          sales 
        GROUP BY
          product
        ORDER BY
          product
      `,
      data: [
        ["A", 300, 0.20689655172413793],
        ["B", 750, 0.5172413793103449],
        ["C", 400, 0.27586206896551724],
      ],
    },
  ],
};

for (const [key, cases] of Object.entries(testCases)) {
  describe(key, () => {
    cases.forEach((testCase) => {
      const tests = Array.isArray(testCase.input)
        ? testCase.input
        : [testCase.input];
      tests.forEach((input) => {
        test(`${input}`, () => {
          const [columns, resultSet] = context.execute(input);
          if (testCase.columns) {
            expect(normalizeColumns(columns)).toEqual(testCase.columns);
          }
          if (testCase.data !== undefined) {
            expect(resultSet).toEqual(
              Array.isArray(testCase.data) ? testCase.data : [[testCase.data]],
            );
          }
        });
      });
    });
  });
}

describe("Window Functions", () => {
  test("Column missing in GROUP BY", () => {
    expect(() =>
      context.execute(`
          SELECT
           product, 
           SUM(SUM(revenue)) OVER (ORDER BY product) 
         FROM sales
         ORDER BY product
      `),
    ).toThrow(
      `column(s) "sales"."product" must appear in the GROUP BY clause or be used in an aggregate function`,
    );
  });

  test("RANGE with text column", () => {
    expect(() =>
      context.execute(`
          SELECT
            product, 
            SUM(revenue) OVER (ORDER BY product RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) 
          FROM sales
          ORDER BY product
      `),
    ).toThrow(
      "RANGE with offset PRECEDING/FOLLOWING is not supported for column type text",
    );
  });

  test("RANGE with multiple order", () => {
    expect(() =>
      context.execute(`
          SELECT
            product, 
            SUM(revenue) OVER (ORDER BY product, revenue RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
          FROM sales
          ORDER BY product
      `),
    ).toThrow(
      "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column",
    );
  });

  test("Aggregate Window Functions", () => {
    const [_, data] = context.execute(`
         SELECT
           product, 
           SUM(revenue) OVER (ORDER BY product) 
         FROM sales
         ORDER BY product
      `);

    expect(data).toEqual([
      ["A", 300],
      ["A", 300],
      ["A", 300],
      ["B", 1050],
      ["B", 1050],
      ["B", 1050],
      ["C", 1450],
      ["C", 1450],
    ]);
  });

  test("Count DISTINCT", () => {
    let response = context.execute(`SELECT count(product) FROM sales`);
    expect(response[1]).toEqual([[8]]);

    response = context.execute(`SELECT count(distinct product) FROM sales`);
    expect(response[1]).toEqual([[3]]);
  });

  test("Aggregate Window Functions", () => {
    const [columns, data] = context.execute(`
        SELECT a, b, group_concat(b, '.') OVER (
          ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS group_concat FROM t1;  
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "a", type: Number },
      { name: "b", type: String },
      { name: "group_concat", type: String },
    ]);

    expect(data).toEqual([
      [1, "A", "A.B"],
      [2, "B", "A.B.C"],
      [3, "C", "B.C.D"],
      [4, "D", "C.D.E"],
      [5, "E", "D.E.F"],
      [6, "F", "E.F.G"],
      [7, "G", "F.G"],
    ]);
  });

  test("PARTITION BY Clause / Row Number", () => {
    const [_, data] = context.execute(`
        SELECT c, a, b, group_concat(b, '.') OVER (win) AS group_concat,
        row_number() OVER ( win ) 
        FROM t1 
        WINDOW win AS ( PARTITION BY c ORDER BY a RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING )
        ORDER BY c, a
      `);
    expect(data).toEqual([
      ["one", 1, "A", "A.D.G", 1],
      ["one", 4, "D", "D.G", 2],
      ["one", 7, "G", "G", 3],
      ["three", 3, "C", "C.F", 1],
      ["three", 6, "F", "F", 2],
      ["two", 2, "B", "B.E", 1],
      ["two", 5, "E", "E", 2],
    ]);
  });

  test("PARTITION BY Clause", () => {
    const [_, data] = context.execute(`
        SELECT c, a, b, group_concat(b, '.') OVER (
          PARTITION BY c ORDER BY a RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS group_concat
        FROM t1 ORDER BY c, a;
      `);
    expect(data).toEqual([
      ["one", 1, "A", "A.D.G"],
      ["one", 4, "D", "D.G"],
      ["one", 7, "G", "G"],
      ["three", 3, "C", "C.F"],
      ["three", 6, "F", "F"],
      ["two", 2, "B", "B.E"],
      ["two", 5, "E", "E"],
    ]);
  });

  test("PARTITION BY Clause order", () => {
    const [_, data] = context.execute(`
        SELECT c, a, b, group_concat(b, '.') OVER (
          PARTITION BY c ORDER BY a RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS group_concat
        FROM t1 ORDER BY a;
      `);
    expect(data).toEqual([
      ["one", 1, "A", "A.D.G"],
      ["two", 2, "B", "B.E"],
      ["three", 3, "C", "C.F"],
      ["one", 4, "D", "D.G"],
      ["two", 5, "E", "E"],
      ["three", 6, "F", "F"],
      ["one", 7, "G", "G"],
    ]);
  });

  test("Frame Boundaries", () => {
    const [_, data] = context.execute(`
        SELECT c, a, b, group_concat(b, '.') OVER (
          ORDER BY c, a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS group_concat
        FROM t1 ORDER BY c, a;
      `);
    expect(data).toEqual([
      ["one", 1, "A", "A.D.G.C.F.B.E"],
      ["one", 4, "D", "D.G.C.F.B.E"],
      ["one", 7, "G", "G.C.F.B.E"],
      ["three", 3, "C", "C.F.B.E"],
      ["three", 6, "F", "F.B.E"],
      ["two", 2, "B", "B.E"],
      ["two", 5, "E", "E"],
    ]);
  });

  test("FILTER Clause", () => {
    const [_, data] = context.execute(`
        SELECT c, a, b, group_concat(b, '.') FILTER (WHERE c!='two') OVER (
          ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS group_concat
        FROM t1 ORDER BY a;
      `);
    expect(data).toEqual([
      ["one", 1, "A", "A"],
      ["two", 2, "B", "A"],
      ["three", 3, "C", "A.C"],
      ["one", 4, "D", "A.C.D"],
      ["two", 5, "E", "A.C.D"],
      ["three", 6, "F", "A.C.D.F"],
      ["one", 7, "G", "A.C.D.F.G"],
    ]);
  });

  test("FILTER Clause", () => {
    const [_, data] = context.execute(`
        SELECT c, a, b, group_concat(b, '.') FILTER (WHERE c!='two') OVER (
          ORDER BY a
        ) AS group_concat
        FROM t1 ORDER BY a;
      `);
    expect(data).toEqual([
      ["one", 1, "A", "A"],
      ["two", 2, "B", "A"],
      ["three", 3, "C", "A.C"],
      ["one", 4, "D", "A.C.D"],
      ["two", 5, "E", "A.C.D"],
      ["three", 6, "F", "A.C.D.F"],
      ["one", 7, "G", "A.C.D.F.G"],
    ]);
  });

  test("EXCLUDE Clause", () => {
    const [columns, data] = context.execute(`
        SELECT c, a, b,
          group_concat(b, '.') OVER (
            ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  EXCLUDE NO OTHERS
          ) AS no_others,
          group_concat(b, '.') OVER (
            ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
          ) AS current_row,
          group_concat(b, '.') OVER (
            ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP
          ) AS grp,
          group_concat(b, '.') OVER (
            ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES
          ) AS ties
        FROM t1 ORDER BY c, a;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "c", type: String },
      { name: "a", type: Number },
      { name: "b", type: String },
      { name: "no_others", type: String },
      { name: "current_row", type: String },
      { name: "grp", type: String },
      { name: "ties", type: String },
    ]);
    expect(data).toEqual([
      ["one", 1, "A", "A.D.G", "D.G", "", "A"],
      ["one", 4, "D", "A.D.G", "A.G", "", "D"],
      ["one", 7, "G", "A.D.G", "A.D", "", "G"],
      ["three", 3, "C", "A.D.G.C.F", "A.D.G.F", "A.D.G", "A.D.G.C"],
      ["three", 6, "F", "A.D.G.C.F", "A.D.G.C", "A.D.G", "A.D.G.F"],
      [
        "two",
        2,
        "B",
        "A.D.G.C.F.B.E",
        "A.D.G.C.F.E",
        "A.D.G.C.F",
        "A.D.G.C.F.B",
      ],
      [
        "two",
        5,
        "E",
        "A.D.G.C.F.B.E",
        "A.D.G.C.F.B",
        "A.D.G.C.F",
        "A.D.G.C.F.E",
      ],
    ]);
  });

  test("Window Functions", () => {
    const [_, data] = context.execute(`
      SELECT b                         AS b,
            lead(b, 2, 'n/a') OVER win AS lead,
            lag(b) OVER win            AS lag,
            first_value(b) OVER win    AS first_value,
            last_value(b) OVER win     AS last_value,
            nth_value(b, 3) OVER win   AS nth_value_3
      FROM t1
      WINDOW win AS (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    `);
    expect(data).toEqual([
      ["A", "C", null, "A", "A", null],
      ["B", "D", "A", "A", "B", null],
      ["C", "E", "B", "A", "C", "C"],
      ["D", "F", "C", "A", "D", "C"],
      ["E", "G", "D", "A", "E", "C"],
      ["F", "n/a", "E", "A", "F", "C"],
      ["G", "n/a", "F", "A", "G", "C"],
    ]);
  });

  test("Window Functions / Rank", () => {
    const [_, data] = context.execute(`
      SELECT a                       AS a,
            row_number() OVER win    AS row_number,
            rank() OVER win          AS rank,
            dense_rank() OVER win    AS dense_rank
      FROM t2
      WINDOW win AS (ORDER BY a);
    `);
    expect(data).toEqual([
      ["a", 1, 1, 1],
      ["a", 2, 1, 1],
      ["a", 3, 1, 1],
      ["b", 4, 4, 2],
      ["c", 5, 5, 3],
      ["c", 6, 5, 3],
    ]);
  });
});
