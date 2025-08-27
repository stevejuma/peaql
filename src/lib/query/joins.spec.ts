import { describe, test, expect } from "vitest";
import { Context } from "./context";
import { AttributeColumn } from "./nodes";
import { Table } from "./models";
import { INTEGER, normalizeColumns } from "./types";

describe("Joins", () => {
  const context = new Context().withTables(
    Table.create(
      "employees",
      new AttributeColumn("employee_id", Number),
      new AttributeColumn("first_name", String),
      new AttributeColumn("last_name", String),
      new AttributeColumn("position_id", Number),
    ).data([
      {
        employee_id: 1000,
        first_name: "John",
        last_name: "Smith",
        position_id: 1,
      },
      {
        employee_id: 1001,
        first_name: "Dave",
        last_name: "Anderson",
        position_id: 2,
      },
      {
        employee_id: 1002,
        first_name: "John",
        last_name: "Doe",
        position_id: 3,
      },
      { employee_id: 1003, first_name: "Dylen", last_name: "Hunt" },
    ]),
    Table.create(
      "positions",
      new AttributeColumn("position_id", Number),
      new AttributeColumn("title", String),
    ).data([
      { position_id: 1, title: "Manager" },
      { position_id: 2, title: "Project Planner" },
      { position_id: 3, title: "Programmer" },
      { position_id: 4, title: "Data Analyst" },
    ]),
    Table.create(
      "departments",
      new AttributeColumn("department_id", Number),
      new AttributeColumn("department_name", String),
    ).data([
      { department_id: 30, department_name: "HR" },
      { department_id: 999, department_name: "Sales" },
    ]),
  );

  test("INNER JOIN", () => {
    const [columns, data] = context.execute(`
        SELECT employees.employee_id, employees.last_name, title
        FROM employees
        INNER JOIN positions
        ON employees.position_id = positions.position_id;
      `);

    expect(normalizeColumns(columns)).toEqual([
      { name: "employee_id", type: Number },
      { name: "last_name", type: String },
      { name: "title", type: String },
    ]);
    expect(data).toEqual([
      [1000, "Smith", "Manager"],
      [1001, "Anderson", "Project Planner"],
      [1002, "Doe", "Programmer"],
    ]);
  });

  test("CTE", () => {
    const [columns, data] = context.execute(`
        with cte as (select 1 as x, 2 as x)
        select * from cte; 
      `);
    expect(normalizeColumns(columns)).toEqual([{ name: "x", type: INTEGER }]);
    expect(data).toEqual([[2]]);
  });

  test("JOIN with alias", () => {
    const [columns, data] = context.execute(`
        SELECT e.employee_id, e.last_name, p.title
        FROM employees e
        JOIN positions p
        ON e.position_id = p.position_id;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "employee_id", type: Number },
      { name: "last_name", type: String },
      { name: "title", type: String },
    ]);
    expect(data).toEqual([
      [1000, "Smith", "Manager"],
      [1001, "Anderson", "Project Planner"],
      [1002, "Doe", "Programmer"],
    ]);
  });

  test("JOIN", () => {
    const [columns, data] = context.execute(`
        SELECT employees.employee_id, employees.last_name, positions.title
        FROM employees 
        JOIN positions
        ON employees.position_id = positions.position_id;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "employee_id", type: Number },
      { name: "last_name", type: String },
      { name: "title", type: String },
    ]);
    expect(data).toEqual([
      [1000, "Smith", "Manager"],
      [1001, "Anderson", "Project Planner"],
      [1002, "Doe", "Programmer"],
    ]);
  });

  test("JOIN SubQuery", () => {
    const [columns, data] = context.execute(`
        SELECT employees.employee_id, employees.last_name, p.title
        FROM employees
        JOIN (
            SELECT * FROM positions
        ) AS p
        ON employees.position_id = p.position_id;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "employee_id", type: Number },
      { name: "last_name", type: String },
      { name: "title", type: String },
    ]);
    expect(data).toEqual([
      [1000, "Smith", "Manager"],
      [1001, "Anderson", "Project Planner"],
      [1002, "Doe", "Programmer"],
    ]);
  });

  test("JOIN CTE", () => {
    const [columns, data] = context.execute(`
        WITH p AS (
          SELECT * FROM positions
        )
        SELECT employees.employee_id, employees.last_name, p.title
        FROM employees
        JOIN p ON employees.position_id = p.position_id;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "employee_id", type: Number },
      { name: "last_name", type: String },
      { name: "title", type: String },
    ]);
    expect(data).toEqual([
      [1000, "Smith", "Manager"],
      [1001, "Anderson", "Project Planner"],
      [1002, "Doe", "Programmer"],
    ]);
  });

  test("JOIN USING", () => {
    const [columns, data] = context.execute(`
        SELECT employees.employee_id, employees.last_name, positions.title
        FROM employees 
        JOIN positions USING (position_id)
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "employee_id", type: Number },
      { name: "last_name", type: String },
      { name: "title", type: String },
    ]);
    expect(data).toEqual([
      [1000, "Smith", "Manager"],
      [1001, "Anderson", "Project Planner"],
      [1002, "Doe", "Programmer"],
    ]);
  });

  test("LEFT OUTER JOIN", () => {
    const [columns, data] = context.execute(`
        SELECT employees.employee_id, employees.last_name, positions.title
        FROM employees 
        LEFT OUTER JOIN positions
        ON employees.position_id = positions.position_id;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "employee_id", type: Number },
      { name: "last_name", type: String },
      { name: "title", type: String },
    ]);
    expect(data).toEqual([
      [1000, "Smith", "Manager"],
      [1001, "Anderson", "Project Planner"],
      [1002, "Doe", "Programmer"],
      [1003, "Hunt", null],
    ]);
  });

  test("CROSS JOIN", () => {
    const [columns, data] = context.execute(`
        SELECT *
        FROM positions 
        CROSS JOIN departments ON true;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "position_id", type: Number },
      { name: "title", type: String },
      { name: "department_id", type: Number },
      { name: "department_name", type: String },
    ]);
    expect(data).toEqual([
      [1, "Manager", 30, "HR"],
      [1, "Manager", 999, "Sales"],
      [2, "Project Planner", 30, "HR"],
      [2, "Project Planner", 999, "Sales"],
      [3, "Programmer", 30, "HR"],
      [3, "Programmer", 999, "Sales"],
      [4, "Data Analyst", 30, "HR"],
      [4, "Data Analyst", 999, "Sales"],
    ]);
  });

  test("CROSS JOIN INNER", () => {
    const [columns, data] = context.execute(`
        SELECT employees.employee_id, employees.last_name, positions.title
        FROM employees 
        CROSS JOIN positions
        ON employees.position_id = positions.position_id;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "employee_id", type: Number },
      { name: "last_name", type: String },
      { name: "title", type: String },
    ]);
    expect(data).toEqual([
      [1000, "Smith", "Manager"],
      [1001, "Anderson", "Project Planner"],
      [1002, "Doe", "Programmer"],
    ]);
  });

  test("CROSS JOIN WHERE", () => {
    const [columns, data] = context.execute(`
        SELECT employees.employee_id, employees.last_name, positions.title
        FROM employees 
        CROSS JOIN positions
        WHERE employees.position_id = positions.position_id;
      `);
    expect(normalizeColumns(columns)).toEqual([
      { name: "employee_id", type: Number },
      { name: "last_name", type: String },
      { name: "title", type: String },
    ]);
    expect(data).toEqual([
      [1000, "Smith", "Manager"],
      [1001, "Anderson", "Project Planner"],
      [1002, "Doe", "Programmer"],
    ]);
  });
});
