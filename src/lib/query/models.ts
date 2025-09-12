/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  AttributeColumn,
  AttributeGetter,
  EvalColumn,
  EvalGetter,
  EvalQuery,
} from "./nodes";
import {
  DType,
  EvalNode,
  getValueByDotNotation,
  isNull,
  isSameType,
  NULL,
  Structure,
  structureFor,
  typeCast,
  typeFor,
  typeName,
  typeOf,
} from "./types";
import { CompilationError, InternalError } from "../errors";
import { Expression, Query } from "../parser";
import { Context } from "./context";

export type TableProps = {
  data: unknown[] | (() => unknown[]);
  parent?: Table;
};

export type MutableTableProperties = {
  name: string;
  columns: Record<string, EvalNode> | Map<string, EvalNode> | AttributeColumn[];
  joins: Map<string, Table>;
  wildcardColumns: string[];
  props: Partial<TableProps>;
  context: any;
  constraints: Array<TableConstraint>;
};

export type TableConstraint = {
  name: string;
  constraint: Expression;
  expr: EvalNode;
  column?: string;
};

export type TableModel = {
  name: string;
  columns: Array<{ name: string; type: string }>;
  constraints: Array<{ column?: string; expr: string; name: string }>;
  data: Array<unknown>;
};

export const TableColumns = Symbol("Columns");

export function Column(type: DType, name?: string) {
  return function <T extends Table, Args extends any[], Return>(
    target: (this: T, context: any) => Return,
    context: ClassMethodDecoratorContext<T, (this: T, ...args: Args) => Return>,
  ) {
    const key = name ?? String(context.name);

    // Store the original method
    const originalMethod = target;

    // Add to context.addInitializer to run when the class is constructed
    context.addInitializer(function (this: T) {
      const wrappedGetter = (contextParam: unknown) => {
        try {
          let value = originalMethod.call(this, contextParam as Args[0]);
          if (isNull(value)) {
            value = getValueByDotNotation(contextParam, key) as Return;
          }
          return value;
        } catch (_) {
          return getValueByDotNotation(contextParam, key) as Return;
        }
      };

      // Initialize the TableColumns map if it doesn't exist
      (this as any)[TableColumns] ||= new Map<string, EvalNode>();
      (this as any)[TableColumns].set(
        key,
        new AttributeGetter(key, type, wrappedGetter),
      );
    });

    // Return the original method unchanged
    return target;
  };
}

export function EntityTable<T extends { new (...args: any[]): any }>(Base: T) {
  return class extends Base {
    constructor(...args: any[]) {
      super(...args);
      const columns = (Base.prototype[TableColumns] ??
        (this as any)[TableColumns]) as Map<string, EvalNode>;
      if (columns) {
        const map = this.columns as Map<string, EvalNode>;
        for (const [key, value] of columns.entries()) {
          map.set(key, value);
        }
      }
    }
  };
}

export class Table {
  readonly columns = new Map<string, EvalNode>();
  readonly joins = new Map<string, Table>();
  public static dataType: DType = Object;

  constructor(
    readonly name: string,
    columns:
      | Record<string, EvalNode>
      | Map<string, EvalNode>
      | EvalColumn[] = {},
    readonly constraints: Array<TableConstraint> = [],
    readonly wildcards: string[] = [],
    readonly props: TableProps = { data: [] },
  ) {
    if (Array.isArray(columns)) {
      columns.forEach((column) => this.columns.set(column.column, column));
    } else {
      const entries =
        columns instanceof Map
          ? [...columns.entries()]
          : Object.entries(columns);
      for (const [key, value] of entries) {
        this.columns.set(key, value);
      }
    }
  }

  get parent(): Table | undefined {
    return this.props.parent;
  }

  getColumn(name: string, fullScan: boolean = true): EvalNode {
    const columns: EvalNode[] = [];

    const column = this.columns.get(name);
    if (column) {
      if (!fullScan) {
        return column;
      }
      columns.push(column);
    }
    for (const [k, v] of this.joins.entries()) {
      const structure = structureFor(this.columns.get(k)?.type);
      if (structure && structure.columns.has(name)) {
        const getter = structure.columns.get(name);
        columns.push(new EvalGetter(this.columns.get(k)!, getter, getter.type));
      } else {
        const column = v.getColumn(name);
        if (column) {
          columns.push(column);
        }
      }
    }

    if (columns.length > 1) {
      throw new InternalError(`ambiguous identifier "${name}"`);
    }
    return columns[0];
  }

  toStructure(type: "default" | "join" = "default"): typeof Structure {
    const tableName = this.name;
    const tableColumns = new Map([...this.columns.entries()]);

    return class extends Structure {
      public static name = tableName;
      public static type: "default" | "join" = type;
      public static columns = tableColumns;
    };
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  setContext(context: any) {}

  get wildcardColumns() {
    if (this.wildcards.length) {
      return this.wildcards;
    }
    return [...this.columns.keys()];
  }

  prepare() {
    if (Array.isArray(this.props.data)) {
      return [...this.props.data];
    }
    return this.props.data();
  }

  *[Symbol.iterator]() {
    for (const entry of this.prepare()) {
      yield entry;
    }
  }

  get rows() {
    return [...this.prepare()];
  }

  copy(props: Partial<MutableTableProperties> = {}) {
    const table = new Table(
      props.name ?? this.name,
      props.columns ?? this.columns,
      props.constraints ?? this.constraints,
      props.wildcardColumns ?? this.wildcards,
      props.props ? { ...this.props, ...props.props } : { ...this.props },
    );

    const entries = props.joins ? props.joins.entries() : this.joins.entries();
    for (const [k, v] of entries) {
      table.joins.set(k, v);
    }
    return table;
  }

  update(props: Partial<TableProps> = {}) {
    return this.copy({ props });
  }

  data<T>(data: T[] | (() => unknown[])) {
    return this.update({ data });
  }

  validateData(): this {
    const records = (
      Array.isArray(this.props.data) ? this.props.data : this.props.data()
    ) as Array<Record<string, unknown>>;
    for (const row of records) {
      for (const [name, column] of this.columns.entries()) {
        const value = column.resolve(row);
        const valueType = typeOf(value);
        if (!isSameType(valueType, column.type) && valueType !== NULL) {
          const coerced = typeCast(value, column.type);
          if (!isSameType(typeOf(coerced), column.type)) {
            throw new CompilationError(
              `Failing row contains (${Object.values(row)
                .map((it) => (isNull(it) ? "null" : it))
                .join(
                  ", ",
                )}). invalid input syntax for type ${typeName(column.type)}: ${JSON.stringify(value)} for column "${name}"`,
            );
          } else {
            row[name] = coerced;
          }
        }
      }

      for (const constraint of this.constraints) {
        const value = constraint.expr.resolve(row);
        if (value === false || isNull(value)) {
          if (constraint.name === "not-null" && constraint.column) {
            throw new CompilationError(
              `Failing row contains (${Object.values(row)
                .map((it) => (isNull(it) ? "null" : it))
                .join(
                  ", ",
                )}). null value in column "${constraint.column}" of relation "${this.name}" violates not-null constraint`,
            );
          }
          if (value === true || isNull(value)) continue;
          throw new CompilationError(
            `Failing row contains (${Object.values(row)
              .map((it) => (isNull(it) ? "null" : it))
              .join(
                ", ",
              )}). new row for relation "${this.name}" violates check constraint "${constraint.name}"`,
          );
        }
      }
    }

    return this;
  }

  toJSON(): TableModel {
    const record: TableModel = {
      name: this.name,
      columns: [...this.columns.entries()].map(([k, v]) => ({
        name: k,
        type: typeName(v.type),
      })),
      constraints: this.constraints.map((constraint) => {
        return {
          name: constraint.name,
          column: constraint.column,
          expr: constraint.constraint.toString(),
        };
      }),
      data: [...this.rows],
    };
    return record;
  }

  static create(name: string, ...columns: AttributeColumn[]): Table {
    return new Table(name, columns);
  }

  static determineTypes(records: Array<Record<string, unknown>>) {
    const columns: Record<string, Set<DType>> = {};
    for (const record of records) {
      for (const [key, value] of Object.entries(record)) {
        const type = typeOf(value);
        columns[key] ||= new Set<DType>();
        columns[key].add(type);
      }
    }
    const columnTypes: Record<string, DType> = {};
    for (const [key, types] of Object.entries(columns)) {
      const validTypes = [...types].filter((t) => t !== NULL);
      if (validTypes.length == 1) {
        columnTypes[key] = validTypes[0];
        continue;
      }
      columnTypes[key] = Object;
    }
    return columnTypes;
  }

  static fromJSON(
    model: TableModel,
    options: Partial<MutableTableProperties> = {},
  ): Table {
    const columns = model.columns.map(
      (col) => new AttributeColumn(col.name, typeFor(col.type)),
    );
    const table = Table.create(model.name, ...columns);
    const context = Context.create(table).withDefaultTable(model.name);

    const constraints: TableConstraint[] = model.constraints.map((c) => {
      const stmt = context.prepare("SELECT " + c.expr);
      const expr = stmt.expr;
      if (!(expr instanceof Query)) {
        throw new CompilationError(`Invalid constraint: ${c.expr}`, expr);
      }
      return {
        name: c.name,
        column: c.column,
        constraint: expr.select.targets[0].expression,
        expr: context.compile(expr.select.targets[0].expression),
      };
    });

    const records = model.data;

    return table
      .copy({
        ...(options || {}),
        constraints,
      })
      .data(records)
      .validateData();
  }
}

export class SubQueryTable extends Table {
  constructor(
    readonly query: EvalQuery,
    name: string = "",
  ) {
    super(name, query.subQueryColumns());
  }

  #rows: any[] | null = null;
  get rows() {
    if (this.#rows) {
      return this.#rows;
    }
    const [columns, rows] = this.query.resolve();
    const data: any = rows.map((it) => {
      return it.reduce((row: Record<symbol, any>, value, i) => {
        const column = columns[i];
        if (column.name) {
          row[column.name] = value;
        }
        return row;
      }, {});
    });
    this.props.data = data;
    this.#rows = data;
    return this.#rows;
  }

  *[Symbol.iterator]() {
    for (const entry of this.rows) {
      yield entry;
    }
  }
}
