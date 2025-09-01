import {
  parseQuery,
  LiteralExpression,
  Expression,
  StatementExpression,
  TableModificationExpression,
} from "../parser";
import { Compiler, CompilerOptions } from "./compiler";
import { Table } from "./models";
import { DateTime, Duration } from "luxon";
import { ParseError, StatementError } from "../errors";
import { EvalNode } from "./types";
type Constant = number | string | boolean | null | DateTime | Duration;

export interface PreparedStatment {
  query: string;
  expr: Expression;
  errors: ParseError[];
  settings: Record<string, Constant | Array<Constant>>;
}

export class Context {
  readonly tables = new Map<string, Table>();
  readonly settings: Record<string, Constant | Array<Constant>> = {};
  readonly compilerOptions: Partial<CompilerOptions> = {};
  compiler!: Compiler;

  private constructor(
    readonly errors: Error[] = [],
    ...tables: Table[]
  ) {
    tables.forEach((table) => {
      table.setContext(this);
      this.tables.set(table.name, table);
    });
  }

  withDefaultTable(name: string): this {
    this.compilerOptions.defaultTableName = name;
    return this;
  }

  static create(...tables: Table[]): Context {
    return new Context([], ...tables);
  }

  withTables(...tables: Table[]) {
    tables.forEach((table) => {
      table.setContext(this);
      this.tables.set(table.name, table);
    });
    return this;
  }

  copy(
    props: Partial<{
      errors: Error[];
      settings: Record<string, Constant | Array<Constant>>;
    }> = {},
  ) {
    const context = new Context(props.errors ?? [...this.errors]).withTables(
      ...[...this.tables.values()].map((it) => {
        return it;
      }),
    );

    for (const [key, value] of Object.entries({
      ...(props.settings || this.settings),
    })) {
      context.settings[key] = value;
    }
    return context;
  }

  prepare(query: string): PreparedStatment {
    const [expr, errors, options] = parseQuery(query);
    const settings = options.reduce(
      (acc: Record<string, Constant | Array<Constant>>, option) => {
        if (option.value instanceof LiteralExpression) {
          acc[option.name] = option.value.value;
        } else {
          acc[option.name] = option.value.values.map((it) => it.value);
        }
        return acc;
      },
      {},
    );
    return {
      query,
      expr,
      errors,
      settings,
    };
  }

  public isTableModification(expr: Expression) {
    return (
      expr instanceof TableModificationExpression ||
      (expr instanceof StatementExpression &&
        expr.statements.some(this.isTableModification))
    );
  }

  compile(
    statement: string | PreparedStatment | Expression,
    parameters?: Record<string, Constant | Array<Constant>>,
    options?: Partial<CompilerOptions>,
  ): EvalNode {
    if (typeof statement === "string") {
      return this.compile(this.prepare(statement), parameters, options);
    }

    if ("errors" in statement && statement.errors.length) {
      throw new StatementError("Error processing statement", statement);
    }

    const expr = statement instanceof Expression ? statement : statement.expr;

    if (this.isTableModification(expr)) {
      return new Compiler(this, options ?? this.compilerOptions).compile(
        expr,
        parameters,
      );
    }

    const context =
      statement instanceof Expression
        ? this.copy()
        : this.copy({ settings: statement.settings });
    return new Compiler(context, options ?? this.compilerOptions).compile(
      expr,
      parameters,
    );
  }

  execute(
    query: string | PreparedStatment,
    parameters?: Record<string, Constant | Array<Constant>>,
    options?: Partial<CompilerOptions>,
  ) {
    return this.compile(
      query,
      parameters,
      options ?? this.compilerOptions,
    ).resolve();
  }
}
