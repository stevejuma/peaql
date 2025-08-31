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
  typeName,
  typeOf,
} from "./types";
import { CompilationError, InternalError } from "../errors";

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
  expr: EvalNode;
  column?: string;
};

export const TableColumns = Symbol("Columns");

export function Column(type: DType, name?: string) {
  return function (
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor,
  ) {
    const key = name ?? propertyKey;
    const getter = (descriptor.value as (context: unknown) => unknown).bind(
      target,
    );
    const wrappedGetter = (context: unknown) => {
      try {
        let value = getter(context);
        if (isNull(value)) {
          value = getValueByDotNotation(context, key);
        }
        return value;
      } catch (_) {
        return getValueByDotNotation(context, key);
      }
    };
    target[TableColumns] ||= new Map<string, EvalNode>();
    target[TableColumns].set(
      key,
      new AttributeGetter(key, type, wrappedGetter),
    );
    return descriptor;
  };
}

export function EntityTable<T extends { new (...args: any[]): any }>(Base: T) {
  return class extends Base {
    constructor(...args: any[]) {
      super(...args);
      const columns = Base.prototype[TableColumns] as Map<string, EvalNode>;
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

  static fromObject(
    name: string,
    records: Array<Record<string, unknown>>,
    props?: Partial<MutableTableProperties>,
  ): Table {
    let columns: MutableTableProperties["columns"] = props?.columns;
    if (!columns) {
      const columnTypes: Record<string, EvalNode> = {};
      for (const [key, type] of Object.entries(this.determineTypes(records))) {
        if (type instanceof EvalNode) {
          columnTypes[key] = type;
        } else {
          columnTypes[key] = new AttributeColumn(key, type);
        }
      }
      columns = columnTypes;
    }

    const table = new Table(name, columns).data(records);

    for (const row of records) {
      for (const [name, column] of table.columns.entries()) {
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

      for (const constraint of table.constraints) {
        const value = constraint.expr.resolve(row);
        if (value === false || isNull(value)) {
          if (constraint.name === "not-null" && constraint.column) {
            throw new CompilationError(
              `Failing row contains (${Object.values(row)
                .map((it) => (isNull(it) ? "null" : it))
                .join(
                  ", ",
                )}). null value in column "${constraint.column}" of relation "${table.name}" violates not-null constraint`,
            );
          }
          throw new CompilationError(
            `Failing row contains (${Object.values(row)
              .map((it) => (isNull(it) ? "null" : it))
              .join(
                ", ",
              )}). new row for relation "${table.name}" violates check constraint "${constraint.name}"`,
          );
        }
      }
    }

    if (props) {
      return table.copy(props);
    }
    return table;
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
