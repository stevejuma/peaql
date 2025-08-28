import {
  parseQuery,
  LiteralExpression,
  Expression,
  CreateTableExpression,
  InsertExpression,
  StatementExpression,
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

  constructor(
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

  private isDDL(expr: Expression) {
    return expr instanceof CreateTableExpression || expr instanceof InsertExpression || (expr instanceof StatementExpression && expr.statements.some(this.isDDL));
  }

  compile(
    statement: string | PreparedStatment,
    parameters?: Record<string, Constant | Array<Constant>>,
    options?: Partial<CompilerOptions>,
  ): EvalNode {
    if (typeof statement === "string") {
      return this.compile(this.prepare(statement), parameters, options);
    }

    if (statement.errors.length) {
      throw new StatementError(
        'Error processing statement',
        statement 
      );
    }

    if (
      this.isDDL(statement.expr)
    ) {
      return new Compiler(this, options ?? this.compilerOptions).compile(
        statement.expr,
        parameters,
      );
    }
    return new Compiler(
      this.copy({ settings: statement.settings }),
      options ?? this.compilerOptions,
    ).compile(statement.expr, parameters);
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
