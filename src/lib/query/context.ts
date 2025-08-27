import {
  parseQuery,
  LiteralExpression,
  Expression,
  CreateTableExpression,
  InsertExpression,
} from "../parser";
import { Compiler, CompilerOptions } from "./compiler";
import { Table } from "./models";
import { DateTime, Duration } from "luxon";
import { CompilationError, ParseError } from "../errors";
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

  constructor(
    readonly errors: Error[] = [],
    ...tables: Table[]
  ) {
    tables.forEach((table) => {
      table.setContext(this);
      this.tables.set(table.name, table);
    });
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

  prepare(
    query: string,
  ): PreparedStatment {
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
       query, expr, errors, settings
    };
  }

  compile(
    statement: string | PreparedStatment,
    parameters?: Record<string, Constant | Array<Constant>>,
    options: Partial<CompilerOptions> = {},
  ): EvalNode {
    if (typeof statement === "string") {
      return this.compile(this.prepare(statement), parameters, options);
    }

    if (statement.errors.length) {
      throw new CompilationError(
        `${statement.errors.map((it) => `${statement.query}\n\n${it.options.node}: ${it.message}`).join("\n")}`,
      );
    }
    if (
      statement.expr instanceof CreateTableExpression ||
      statement.expr instanceof InsertExpression
    ) {
      return new Compiler(this, options).compile(statement.expr, parameters);
    }
    return new Compiler(this.copy({ settings: statement.settings }), options).compile(
      statement.expr,
      parameters,
    );
  }

  execute(
    query: string | PreparedStatment,
    parameters?: Record<string, Constant | Array<Constant>>,
    options: Partial<CompilerOptions> = {},
  ) {
    return this.compile(query, parameters, options).resolve();
  }
}