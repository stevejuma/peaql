/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  AttributeColumn,
  AttributeGetter,
  EvalColumn,
  EvalGetter,
  EvalQuery,
} from "./nodes";
import { DateTime } from "luxon";
import {
  DType,
  EvalNode,
  getValueByDotNotation,
  isNull,
  NULL,
  Structure,
  structureFor,
  typeOf,
} from "./types";
import { CompilationError } from "../errors";

export type TableProps = {
  data: unknown[];
  parent?: Table;
  open?: DateTime;
  close?: DateTime | boolean;
  clear?: boolean;
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
      throw new CompilationError(`ambiguous identifier "${name}"`);
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
    return [...this.props.data];
  }

  *[Symbol.iterator]() {
    for (const entry of this.prepare()) {
      yield entry;
    }
  }

  get rows() {
    return [...this.props.data];
  }

  copy(
    props: Partial<{
      name: string;
      columns:
        | Record<string, EvalNode>
        | Map<string, EvalNode>
        | AttributeColumn[];
      joins: Map<string, Table>;
      wildcardColumns: string[];
      props: Partial<TableProps>;
      context: any;
    }> = {},
  ) {
    const table = new Table(
      props.name ?? this.name,
      props.columns ?? this.columns,
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

  data<T>(data: T[]) {
    return this.update({ data });
  }

  static create(name: string, ...columns: AttributeColumn[]): Table {
    return new Table(name, columns);
  }

  static fromObject(name: string, records: Array<Record<string, unknown>>): Table {
    const columns: Record<string, Set<DType>> = {};

    for (const record of records) {
      for (const [key, value] of Object.entries(record)) {
        const type = typeOf(value);
        columns[key] ||= new Set<DType>();
        columns[key].add(type);
      }
    }

    const columnTypes: Record<string, EvalNode> = {}
    for(const [key, types] of Object.entries(columns)) {
      const validTypes = [...types].filter(t => t !== NULL);
      if (validTypes.length == 1) {
        columnTypes[key] = new AttributeColumn(key, validTypes[0]);
        continue;
      }
      columnTypes[key] = new AttributeColumn(key, Object);
    }

    return new Table(name, columnTypes);
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
