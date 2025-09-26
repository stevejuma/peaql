import { ConstantValue, isNull } from "../query/types";
import { readString } from "./parser";
import { Constant } from "../query";

export type Op =
  | ">"
  | "="
  | "<"
  | "!="
  | ">="
  | "<="
  | "~"
  | "~*"
  | "!~"
  | "!~*"
  | "?~"
  | "?~*"
  | "OR"
  | "AND"
  | "NOT"
  | "ANY"
  | "ALL"
  | "IN"
  | "NOTIN"
  | "ISNULL"
  | "ISNOTNULL"
  | "BETWEEN"
  | "NOTBETWEEN"
  | "+"
  | "-"
  | "*"
  | "%"
  | "/"
  | "NEGATE";

export interface ParseInfo {
  pos: number;
}

export abstract class Expression {
  constructor(readonly parseInfo: ParseInfo) {}

  children(): Expression[] {
    return [];
  }
}

export class ColumnExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly column: string,
  ) {
    super(parseInfo);
  }

  public toString() {
    return this.column;
  }

  get name() {
    return readString(this.column);
  }
}

export class CaseExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly conditions: Array<{ when: Expression; then: Expression }>,
    public value?: Expression,
    public fallback?: Expression,
  ) {
    super(parseInfo);
  }

  public toString() {
    let str = "\n(\n  CASE\n";
    str += this.conditions
      .map((it) => `    WHEN ${it.when} THEN ${it.then}`)
      .join("\n");
    if (this.fallback) {
      str += `\n    ELSE ${this.fallback}`;
    }
    str += "\n  END\n)\n";
    return str;
  }
}

export class OverExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly partitionBy?: Expression,
    readonly orderBy?: OrderByExpression,
    readonly name?: string,
    readonly frame?: {
      type: string;
      preceding: number;
      following: number;
      exclude: string;
    },
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const values: Expression[] = [];
    if (this.partitionBy) {
      values.push(this.partitionBy);
    }
    if (this.orderBy) {
      values.push(this.orderBy);
    }
    return values;
  }

  copy(
    props: Partial<{
      partitionBy: Expression;
      orderBy: OrderByExpression;
      name: string;
      frame: {
        type: string;
        preceding: number;
        following: number;
        exclude: string | "NONE" | "CURRENT" | "GROUP" | "TIES";
      };
    }> = {},
  ) {
    return new OverExpression(
      this.parseInfo,
      props.partitionBy ?? this.partitionBy,
      props.orderBy ?? this.orderBy,
      props.name ?? this.name,
      props.frame ?? this.frame,
    );
  }

  public toString() {
    const str: string[] = [];
    if (this.partitionBy) {
      str.push(`PARTITION BY ${this.partitionBy}`);
    }
    if (this.orderBy) {
      str.push(`ORDER BY ${this.orderBy}`);
    }
    if (this.frame) {
      let value = this.frame.type + " BETWEEN ";
      if (this.frame.preceding === Infinity) {
        value += `UNBOUNDED PRECEEDING `;
      } else {
        value += `${this.frame.preceding} PRECEEDING `;
      }
      value += "AND ";
      if (this.frame.following === Infinity) {
        value += `UNBOUNDED FOLLOWING `;
      } else {
        value += `${this.frame.following} FOLLOWING `;
      }
      str.push(value.trim());
    }
    return (
      "OVER (" + (this.name ? this.name + " " : "") + str.join("  \n") + ")"
    );
  }
}

export class JoinExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly type: string,
    readonly table: Expression,
    readonly condition: Expression,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    return [
      this.table,
      this.condition,
      ...this.table.children(),
      ...this.condition.children(),
    ];
  }

  public toString() {
    return `${this.type} JOIN ${this.table} ON ${this.condition}`;
  }
}

export class WildcardExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly column: string,
  ) {
    super(parseInfo);
  }

  public toString() {
    return this.column + ".*";
  }
}

export class OptionExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly name: string,
    readonly value: LiteralExpression | ListExpression | ColumnExpression,
  ) {
    super(parseInfo);
  }

  public toString() {
    return `SET ${this.name} = ${this.value};`;
  }

  children(): Expression[] {
    return [this.value];
  }
}

export class AsteriskExpression extends Expression {
  constructor(parseInfo: ParseInfo) {
    super(parseInfo);
  }

  public toString() {
    return "*";
  }
}

export class TableExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly table: string,
    readonly alias: string = "",
  ) {
    super(parseInfo);
  }

  get name(): string {
    if (this.alias) {
      return this.alias;
    }
    return this.table;
  }

  public toString() {
    return `${this.table}${this.alias ? " AS " + this.alias : ""}`;
  }
}

export class LiteralExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly value: Constant,
  ) {
    super(parseInfo);
  }

  public toString() {
    if (isNull(this.value)) {
      return "null";
    } else if (typeof this.value === "string") {
      return `'${this.value.replace("'", "''")}'`;
    }
    return this.value.toString();
  }
}

export class PlaceHolderExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    public name: string = "",
  ) {
    super(parseInfo);
  }

  public toString() {
    return "?";
  }
}

export class CollectionExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly values: Array<Expression>,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    return this.values;
  }

  public toString() {
    return `(${this.values.join(", ")})`;
  }
}

export class ListExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly values: Array<LiteralExpression | ListExpression>,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    return this.values;
  }

  get value(): ConstantValue {
    return this.values.map(it => it.value) as ConstantValue; 
  }

  public toString() {
    return `[${this.values.join(",")}]`;
  }
}

export class CastExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly type: string,
    readonly expr: Expression,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    return [this.expr];
  }

  public toString() {
    return `(${this.expr})::${this.type}`;
  }
}

export class FunctionExpression extends Expression {
  readonly name: string;
  constructor(
    parseInfo: ParseInfo,
    name: string,
    readonly args: Array<Expression> = [],
    readonly distinct: boolean = false,
    public filter?: Expression,
    public window?: OverExpression,
  ) {
    super(parseInfo);
    this.name = name;
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    this.args.forEach((it) => {
      children.add(it);
      it.children().forEach((c) => children.add(c));
    });
    return [...children];
  }

  public toString() {
    if (!this.args.length) {
      return this.name + "()";
    }
    return `${this.name}(${this.args.join(", ")}) ${this.window || ""}`.trim();
  }
}

export class AllExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly op: Op,
    readonly left: Expression,
    readonly right: Expression,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    [this.right, this.left].forEach((it) => {
      children.add(it);
      it.children().forEach((c) => children.add(c));
    });
    return [...children];
  }

  public toString() {
    return `${this.left} ${this.op} ALL(${this.right})`;
  }
}

export class AnyExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly op: Op,
    readonly left: Expression,
    readonly right: Expression,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    [this.right, this.left].forEach((it) => {
      children.add(it);
      it.children().forEach((c) => children.add(c));
    });
    return [...children];
  }

  public toString() {
    return `${this.left} ${this.op} ANY(${this.right})`;
  }
}

export class StatementExpression extends Expression {
  constructor(readonly statements: Expression[]) {
    super(statements[0].parseInfo);
  }

  toString() {
    return this.statements.map((statement) => statement.toString()).join(";\n");
  }

  children(): Expression[] {
    return this.statements.map((it) => it.children()).flat();
  }
}

export abstract class TableModificationExpression extends Expression {}

export class ColumnDefinition {
  constructor(
    readonly name: string,
    readonly type: string,
    readonly isArray: boolean,
    readonly primaryKey: boolean,
    readonly isNotNull: boolean,
    readonly check?: Expression,
    readonly defaultValue?: Expression,
  ) {}

  toString() {
    return `${this.name} ${this.type}${this.isArray ? "[]" : ""} ${this.isNotNull ? "NOT NULL" : ""}`.trim();
  }
}

export abstract class Constraint {
  constructor(
    readonly table: string,
    readonly type: string,
    readonly name: string = "",
  ) {}

  constraintName(): string {
    return this.name;
  }
}

export class PrimaryKeyConstraint extends Constraint {
  constructor(
    readonly table: string,
    readonly columns: Array<string>,
    name: string = "",
  ) {
    super(table, "PRIMARY KEY", name);
  }

  toString() {
    return `CONSTRAINT ${this.name ? this.name : ""} PRIMARY KEY (${this.columns.join(", ")})`.trim();
  }

  constraintName(): string {
    return (
      this.name ||
      this.table + "_" + this.columns.join("_").toLowerCase() + "_pkey"
    );
  }
}

export class ForeignKeyConstraint extends Constraint {
  constructor(
    readonly table: string,
    readonly columns: Array<string>,
    readonly referenceColumns: Array<string>,
    readonly referenceTable: string,
    name: string = "",
  ) {
    super(table, "FOREIGN KEY", name);
  }

  toString() {
    return `CONSTRAINT ${this.name ? this.name : ""} FOREIGN KEY (${this.columns.join(", ")}) REFERENCES ${this.referenceTable}(${this.referenceColumns.join(", ")})`.trim();
  }

  constraintName(): string {
    return (
      this.name ||
      this.table + "_" + this.columns.join("_").toLowerCase() + "_fkey"
    );
  }
}

export class UniqueConstraint extends Constraint {
  constructor(
    readonly table: string,
    readonly columns: Array<string>,
    name: string = "",
  ) {
    super(table, "UNIQUE", name);
  }

  toString() {
    return `CONSTRAINT ${this.name ? this.name : ""} UNIQUE (${this.columns.join(", ")})`.trim();
  }

  constraintName(): string {
    return (
      this.name ||
      this.table + "_" + this.columns.join("_").toLowerCase() + "_uk"
    );
  }
}

export class CheckConstraint extends Constraint {
  constructor(
    readonly table: string,
    readonly expression: Expression,
    name: string = "",
  ) {
    super(table, "CHECK", name);
  }

  toString() {
    if (!this.name) {
      return `CHECK (${this.expression})`.trim();
    }
    return `CONSTRAINT ${this.name ? this.name : " "} CHECK (${this.expression})`.trim();
  }

  constraintName(): string {
    const columns = this.expression
      .children()
      .filter((it) => it instanceof ColumnExpression)
      .map((it) => it.column);
    return this.name || this.table + "_" + columns.join("_") + "_check";
  }
}

export class UpdateTableExpression extends TableModificationExpression {
  constructor(
    parseInfo: ParseInfo,
    readonly name: string,
    readonly values: Array<Expression>,
    readonly returning: Array<TargetExpression> = [],
    readonly where?: Expression,
  ) {
    super(parseInfo);
  }

  toString() {
    let str = `UPDATE TABLE ${this.name}\nSET\n${this.values.map((it) => "   " + it).join(",")}`;
    if (this.where) {
      str += "\nWHERE " + this.where;
    }
    if (this.returning.length) {
      str += "\n" + this.returning.join(",");
    }
    return str;
  }
}

export class CreateTableExpression extends TableModificationExpression {
  constructor(
    parseInfo: ParseInfo,
    readonly name: string,
    readonly ifNotExists: boolean,
    readonly columns: Array<ColumnDefinition>,
    readonly using: string,
    readonly constraints: Constraint[],
    readonly query?: Query,
  ) {
    super(parseInfo);
  }

  toString() {
    if (this.query) {
      return `CREATE TABLE ${this.name} AS ${this.query}`;
    }
    let str = `CREATE TABLE ${this.name} (\n${this.columns.map((it) => "   " + it.toString()).join(",\n")}`;
    if (this.constraints.length) {
      str += `,\n${this.constraints.map((it) => "   " + it.toString()).join(",\n")}`;
    }
    str += `\n)${this.using ? `"${this.using}"` : ""};`;
    return str;
  }

  children(): Expression[] {
    return [];
  }
}

export class InsertExpression extends TableModificationExpression {
  constructor(
    parseInfo: ParseInfo,
    readonly table: string,
    readonly columns: Array<string>,
    readonly returning: Array<TargetExpression> = [],
    readonly values: Expression[][],
  ) {
    super(parseInfo);
  }

  toString() {
    let str = `INSERT INTO ${this.table} (${this.columns.join(",")}) VALUES\n${this.values.map((it) => `(${it.join(",")})`).join(",\n")}`;
    if (this.returning.length) {
      str += "\n" + this.returning.join(",");
    }
    str += ";";
    return str;
  }
}

export class BooleanExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly op: Op,
    readonly args: Array<Expression>,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    this.args.forEach((it) => {
      children.add(it);
      it.children().forEach((c) => children.add(c));
    });
    return [...children];
  }

  public toString() {
    if (this.op === "NOT" && this.args.length === 1) {
      return `NOT (${this.args[0]})`;
    } else if (this.op === "ISNULL" && this.args.length == 1) {
      return `${this.args[0]} IS NULL`;
    } else if (this.op === "ISNOTNULL" && this.args.length == 1) {
      return `${this.args[0]} IS NOT NULL`;
    } else if (this.args.length === 1) {
      return `${this.op}(${this.args[0]})`;
    } else if (this.op === "BETWEEN") {
      return `${this.args[0]} BETWEEN ${this.args[1]} AND ${this.args[2]}`;
    } else if (this.children().some((it) => it instanceof BooleanExpression)) {
      return `(${this.args.join(" " + this.op + " ")})`;
    }
    return `${this.args.join(" " + this.op + " ")}`;
  }
}

export class TargetExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly expression: Expression,
    readonly as: string = "",
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    children.add(this.expression);
    this.expression.children().forEach((it) => children.add(it));
    return [...children];
  }

  get name(): string {
    if (this.as) {
      return this.as;
    }

    if (this.expression instanceof ColumnExpression) {
      return this.expression.name;
    }

    if (this.expression instanceof AttributeExpression) {
      if (
        this.expression.operand instanceof ColumnExpression &&
        typeof this.expression.name === "string"
      ) {
        return this.expression.name;
      }
    }

    return this.expression.toString();
  }

  public toString() {
    return this.expression + (this.as ? ` AS ${this.as}` : "");
  }
}

export class SelectClause extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly targets: Array<TargetExpression>,
    readonly distinct: boolean = false,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    if (Array.isArray(this.targets)) {
      this.targets.forEach((it) => {
        children.add(it);
        it.children().forEach((c) => children.add(c));
      });
    }
    return [...children];
  }

  public toString() {
    let str = "SELECT\n";
    if (this.targets instanceof AsteriskExpression) {
      str += "  * \n";
    } else {
      str += `  ${this.targets.join(",\n  ")}\n`;
    }
    return str;
  }
}

export class FromClause extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly from: Expression,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    return this.from.children();
  }

  public toString() {
    return `FROM ${this.from}\n`;
  }
}

export class QueryExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly expression: Expression,
    readonly alias: string = "",
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    [this.expression].forEach((it) => {
      children.add(it);
      it.children().forEach((c) => children.add(c));
    });
    return [...children];
  }
}

export class SubQueryExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly query: Query,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    return this.query.children();
  }

  toString() {
    return "(" + this.query.toString() + ")";
  }
}

export class AttributeExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly operand: Expression,
    readonly name: string | FunctionExpression | CastExpression,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = [...this.operand.children()];
    if (this.name instanceof Expression) {
      children.push(...this.name.children());
    }
    return children;
  }

  public toString() {
    let str = "";
    if (this.operand instanceof BooleanExpression) {
      str += `(${this.operand})`;
    } else {
      str += this.operand.toString();
    }
    return str + "." + this.name;
  }
}

export class SubscriptExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly operand: Expression,
    readonly key: string,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    return this.operand.children();
  }

  public toString() {
    return this.operand.toString() + `[${JSON.stringify(this.key)}]`;
  }
}

export class GroupByExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly columns: Array<Expression>,
    public having?: Expression,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    [this.columns, this.having || []].flat().forEach((it) => {
      children.add(it);
      it.children().forEach((c) => children.add(c));
    });
    return [...children];
  }

  public toString() {
    let str = `GROUP BY ${this.columns.join(", ")}`;
    if (this.having) {
      str += ` HAVING ${this.having}`;
    }
    return str;
  }
}

export class OrderExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly column: Expression,
    readonly direction: "ASC" | "DESC" = "ASC",
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    return [this.column];
  }

  public toString() {
    return `${this.column} ${this.direction}`;
  }
}

export class OrderByExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly columns: Array<OrderExpression>,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    this.columns.forEach((it) => {
      children.add(it.column);
      it.column.children().forEach((c) => children.add(c));
    });
    return [...children];
  }

  public toString() {
    return `ORDER BY ${this.columns.join(", ")}`;
  }
}

export class PivotByExpression extends Expression {
  constructor(
    parseInfo: ParseInfo,
    readonly columns: Array<Expression | number>,
  ) {
    super(parseInfo);
  }

  children(): Expression[] {
    const children = new Set<Expression>();
    this.columns
      .filter((it) => it instanceof Expression)
      .forEach((it) => {
        children.add(it);
        it.children().forEach((c) => children.add(c));
      });
    return [...children];
  }

  public toString() {
    return `PIVOT BY ${this.columns.join(", ")}`;
  }
}

export class Query extends Expression {
  public windows: Record<string, OverExpression> = {};
  public joins: JoinExpression[] = [];
  public unions: Array<{ type: string; query: Query }> = [];
  public commonTableExpressions: Record<string, Query> = {};

  constructor(
    public select: SelectClause,
    public from?: FromClause,
    public where?: Expression,
    public groupBy?: GroupByExpression,
    public orderBy?: OrderByExpression,
    public pivotBy?: PivotByExpression,
    public limit?: number,
  ) {
    super({ pos: 0 });
  }

  children(): Expression[] {
    const exprs: Expression[] = [
      ...this.joins.map((it) => [it, it.children()].flat()).flat(),
      ...Object.values(this.windows)
        .map((it) => [it, it.children()].flat())
        .flat(),
      ...Object.values(this.commonTableExpressions)
        .map((it) => [it, it.children()].flat())
        .flat(),
    ];
    const children = new Set<Expression>(
      [
        this.select,
        this.select.children(),
        this.from?.children() ?? [],
        this.where?.children() ?? [],
        this.groupBy?.children() ?? [],
        this.orderBy?.children() ?? [],
        exprs,
      ].flat(),
    );
    return [...children];
  }

  public toString() {
    let str = "";
    const ctes: string[] = [];
    for (const [key, value] of Object.entries(this.commonTableExpressions)) {
      ctes.push(`${key} AS ( ${value} )\n`);
    }

    if (ctes.length) {
      str += "WITH " + ctes.join(",") + "\n";
    }

    str += this.select.toString();
    if (this.from) {
      str += this.from.toString();
    }

    const windows: string[] = [];
    for (const [key, value] of Object.entries(this.windows)) {
      ctes.push(`WINDOW ${key} AS ( ${value} )\n`);
    }

    if (windows.length) {
      str += windows.join("");
    }

    if (this.where) {
      str += "WHERE " + this.where.toString() + "\n";
    }

    if (this.groupBy) {
      str += this.groupBy.toString() + "\n";
    }

    if (this.orderBy) {
      str += this.orderBy.toString() + "\n";
    }

    if (this.pivotBy) {
      str += this.pivotBy.toString() + "\n";
    }

    if (this.limit) {
      str += `LIMIT ${this.limit}\n`;
    }

    return str;
  }
}
