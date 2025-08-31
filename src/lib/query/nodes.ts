/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */

import { DateTime, Duration } from "luxon";
import {
  DType,
  EvalNode,
  typeName,
  typeOf,
  getValueByDotNotation,
  isNull,
  isEqual,
  Operation,
  iterableProduct,
  NULL,
  typeFor,
  typeCast,
  isSameType,
} from "./types";
import { executeSelect } from "./query_execute";
import {
  CompilationError,
  DataError,
  InternalError,
  NotSupportedError,
  ProgrammingError,
} from "../errors";
import {
  BooleanExpression,
  CheckConstraint,
  ColumnExpression,
  CreateTableExpression,
  Expression,
  InsertExpression,
  Op,
  OverExpression,
} from "../parser/ast";
import { Table } from "./models";
import { getColumnsAndAggregates } from "./compiler";
import { Context } from "./context";

export type Constant = number | string | boolean | null | DateTime | Duration;

export class Allocator {
  private size: number = 0;
  constructor() {}

  allocate(): number {
    const handle = this.size;
    this.size += 1;
    return handle;
  }

  createStore() {
    return [...new Array(this.size).fill(null)];
  }
}

const OPERATORS: Partial<Record<Op, Array<Operation>>> = {};
export function findOperator(op: Op, operands: any[]) {
  const matches = (OPERATORS[op] || [])
    .filter((it) => it.matches(operands))
    .sort((a, b) => a.sortKey - b.sortKey);
  return matches[0];
}

const FUNCTIONS: Partial<Record<string, Array<Operation>>> = {};
export function findFunction(fn: string, operands: any[]) {
  const matches = (FUNCTIONS[fn.toLowerCase()] || [])
    .filter((it) => it.matches(operands))
    .sort((a, b) => a.sortKey - b.sortKey);
  return matches[0];
}
export type AggregatorState = "init" | "update" | "finalize";

export function registerOperator(name: Op, ...op: Operation[]) {
  OPERATORS[name] ||= [];
  OPERATORS[name].push(...op);
}

export function createAggregatorFunction(
  name: string,
  inTypes: DType[],
  outType: DType,
  callback: (
    self: EvalAggregator,
    store: any[],
    state: AggregatorState,
    context: any,
    props?: WindowProps,
  ) => void,
) {
  FUNCTIONS[name.toLowerCase()] ||= [];
  FUNCTIONS[name.toLowerCase()].push(
    new Operation(
      inTypes,
      outType,
      () => {
        throw new NotSupportedError("Operator not supported in aggregate");
      },
      (context: any, ...args: EvalNode[]) => {
        const Aggr = class extends EvalAggregator {
          initialize(store: any[]): void {
            super.initialize(store);
            callback(this, store, "init", null);
          }

          update(store: any[], context: any): void {
            callback(this, store, "update", context);
          }

          finalize(store: any[]): void {
            callback(this, store, "finalize", null);
            super.finalize(store);
          }
        };
        const dType =
          outType === Object && args.length === 1 ? args[0].type : outType;
        return new Aggr(context, args, dType);
      },
    ),
  );
}

export function createFunction(
  name: string,
  inTypes: DType[],
  outType: DType,
  operator: (...a: any[]) => any,
  pure: boolean = true,
) {
  FUNCTIONS[name.toLowerCase()] ||= [];
  FUNCTIONS[name.toLowerCase()].push(
    new Operation(
      inTypes,
      outType,
      operator,
      (context: any, ...args: EvalNode[]) => {
        return new EvalFunction(
          pure ? null : context,
          args,
          outType,
          pure,
          operator,
        );
      },
    ),
  );
}

export function unaryOp(
  name: Op,
  operator: (a: any) => any,
  inTypes: DType[],
  outType: DType,
  nullSafe: boolean = false,
) {
  OPERATORS[name] ||= [];
  OPERATORS[name].push(
    new Operation(inTypes, outType, operator, (operand: EvalNode) => {
      return nullSafe
        ? new EvalUnaryOp(operator, outType, operand)
        : new EvalUnaryOpSafe(operator, outType, operand);
    }),
  );
}

export function binaryOp(
  name: Op,
  operator: (a: any, b: any) => any,
  inTypes: DType[],
  outType: DType,
) {
  OPERATORS[name] ||= [];
  OPERATORS[name].push(
    new Operation(
      inTypes,
      outType,
      operator,
      (left: EvalNode, right: EvalNode) => {
        return new EvalBinaryOp(operator, left, right, outType);
      },
    ),
  );
}

export class EvalConstant extends EvalNode {
  constructor(
    readonly value: Constant | Array<Constant>,
    type?: DType,
  ) {
    super(type ?? typeOf(value));
  }

  resolve(_: any): Constant | Array<Constant> {
    return this.value;
  }

  get childNodes(): EvalNode[] {
    return [];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalConstant)) {
      return false;
    } else if (this.value instanceof DateTime) {
      return (
        obj.value instanceof DateTime &&
        obj.value.toMillis() === this.value.toMillis()
      );
    } else if (Array.isArray(this.value)) {
      const arr = obj.value;
      if (Array.isArray(arr)) {
        return (
          this.type === obj.type &&
          arr.length === this.value.length &&
          this.value.every((v, i) => {
            if (v instanceof DateTime) {
              return (
                arr[i] instanceof DateTime && arr[i].toMillis() === v.toMillis()
              );
            }
            return v === arr[i];
          })
        );
      }
      return false;
    }
    return this.type === obj.type && obj.value === this.value;
  }
}

export class EvalColumn extends EvalNode {
  constructor(
    readonly column: string,
    type: DType,
  ) {
    super(type);
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalColumn)) {
      return false;
    }
    return obj.type === this.type && obj.column === this.column;
  }

  resolve(context: any): any {
    return getValueByDotNotation(context, this.column);
  }

  get childNodes(): EvalNode[] {
    return [];
  }
}

export class EvalInsert extends EvalNode {
  constructor(
    readonly node: InsertExpression,
    readonly table: Table,
    readonly values: Record<string, EvalNode>[],
  ) {
    super(EvalInsert);
  }

  get childNodes(): EvalNode[] {
    return this.values.map((it) => Object.values(it)).flat();
  }

  resolve(context?: any): [{ name: symbol; type: DType }[], unknown[][]] {
    const columns: Record<string, DType> = {};
    const rows: Array<Array<unknown>> = [];
    const data = this.table.props.data;
    if (!Array.isArray(data)) {
      throw new InternalError(`Invalid data for table "${this.table.name}"`);
    }

    this.values.forEach((item) => {
      const row: Record<string, unknown> = {};
      const record: unknown[] = [];
      for (const [key, value] of Object.entries(item)) {
        columns[key] ||= this.table.getColumn(key).type;
        row[key] = value.resolve(context);
        const type = typeOf(row[key]);
        if (!isSameType(type, columns[key]) && type !== NULL) {
          const coerced = typeCast(row[key], columns[key]);
          if (!isSameType(typeOf(coerced), columns[key])) {
            throw new CompilationError(
              `invalid input syntax for type ${typeName(columns[key])}: ${JSON.stringify(row[key])}`,
              this.node,
            );
          } else {
            row[key] = coerced;
          }
        }
        record.push(row[key]);
      }

      for (const constraint of this.table.constraints) {
        const value = constraint.expr.resolve(row);
        if (value === false || isNull(value)) {
          if (constraint.name === "not-null" && constraint.column) {
            throw new CompilationError(
              `Failing row contains (${record.map((it) => (isNull(it) ? "null" : it)).join(", ")}). null value in column "${constraint.column}" of relation "${this.table.name}" violates not-null constraint`,
            );
          }
          throw new CompilationError(
            `Failing row contains (${record.map((it) => (isNull(it) ? "null" : it)).join(", ")}). new row for relation "${this.table.name}" violates check constraint "${constraint.name}"`,
          );
        }
      }

      rows.push(record);
      data.push(row);
    });

    const keys = Object.keys(columns).map((k) => Symbol(k));
    return [
      keys.map((name) => ({ name, type: columns[name.description] })),
      rows,
    ];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalInsert)) {
      return false;
    }
    return (
      this.table.name == obj.table.name && isEqual(this.values, obj.values)
    );
  }
}

export class EvalFunction extends EvalNode {
  constructor(
    context: any,
    readonly operands: EvalNode[],
    type: DType,
    readonly pure: boolean = false,
    readonly operator?: (...args: any[]) => any,
  ) {
    super(type, context);
  }

  get childNodes(): EvalNode[] {
    return this.operands;
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalFunction)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.pure === this.pure &&
      this.operator?.name === obj.operator?.name &&
      this.operands.length === obj.operands.length &&
      this.operands.every((it, i) => it.isEqual(obj.operands[i]))
    );
  }

  resolve(context?: any) {
    if (!this.operator) {
      throw new Error(`No operator defined for function`);
    }
    const args = this.operands.map((it) => it.resolve(context));
    if (args.some(isNull)) {
      return null;
    }
    let ctx = this.context;
    if (ctx instanceof EvalNode) {
      ctx = ctx.resolve(context);
    }

    return this.pure ? this.operator(...args) : this.operator(ctx, ...args);
  }
}

export type EvalOver = {
  expression: OverExpression;
  partitionBy?: number[];
  orderBy?: [number, "ASC" | "DESC"][];
  frame: {
    type: string;
    preceding: number;
    following: number;
    exclude: string;
  };
};

export type WindowProps = {
  data: any[];
  index: number;
  fullPartition: any[];
  orderValue: (a: any) => any;
};

export class EvalAggregator extends EvalFunction {
  public distinct: boolean = false;
  handle!: number;
  value: any = null;
  windowState?: WindowProps;
  resolver: (context: any) => any;

  constructor(
    context: any,
    operands: EvalNode[],
    type?: DType | null,
    public filter?: EvalNode,
  ) {
    super(context, operands, type ?? operands[0].type, false);
  }

  get childNodes(): EvalNode[] {
    return [...this.operands, this.filter ?? []].flat();
  }

  allocate(allocator: Allocator) {
    this.handle = allocator.allocate();
  }

  accept(context?: any) {
    return !this.filter || this.filter.resolve(context) ? true : false;
  }

  initialize(store: any[]) {
    this.value = null;
    this.windowState = null;
  }

  update(store: any[], context: any) {}

  finalize(store: any[]) {
    this.value = store[this.handle];
  }

  resolve(context?: any) {
    if (this.resolver) {
      return this.resolver(context);
    }
    return this.value;
  }

  execute(data: any[]) {
    this.handle = 0;
    const store: any[] = [];
    this.initialize(store);
    data.forEach((context) => {
      this.update(store, context);
    });
    this.finalize(store);
    return this.value;
  }
}

export class EvalGetItem extends EvalNode {
  constructor(
    readonly operand: EvalNode,
    readonly key: string,
  ) {
    super(Object);
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalGetItem)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.key === this.key &&
      obj.operand.isEqual(this.operand)
    );
  }

  get childNodes() {
    return [this.operand];
  }

  resolve(context: any) {
    const operand = this.operand.resolve(context);
    if (!operand) return null;
    return operand[this.key];
  }
}

export class EvalGetter extends EvalNode {
  constructor(
    readonly operand: EvalNode,
    readonly getter: EvalNode,
    type: DType,
  ) {
    super(type);
  }

  get childNodes(): EvalNode[] {
    return [this.operand, this.getter];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalGetter)) {
      return false;
    }
    return obj.type === this.type && obj.operand.isEqual(this.operand);
  }

  resolve(context?: any) {
    const operand = this.operand.resolve(context);
    if (isNull(operand)) {
      return null;
    }
    return this.getter.resolve(operand);
  }
}

export class EvalCase extends EvalNode {
  constructor(
    readonly operands: Array<{ when: EvalNode; then: EvalNode }>,
    readonly fallback?: EvalNode,
  ) {
    super(operands[0].then.type);
  }

  get childNodes(): EvalNode[] {
    return [
      this.fallback ? [this.fallback] : [],
      ...this.operands.map((it) => [it.when, it.then]),
    ].flat();
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalCase)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.operands.length === this.operands.length &&
      isEqual(this.operands, obj.operands) &&
      isEqual(this.fallback, obj.fallback)
    );
  }

  resolve(context?: any) {
    return (
      (
        this.operands.find((it) => it.when.resolve(context))?.then ??
        this.fallback
      )?.resolve(context) ?? null
    );
  }
}

export class EvalCoalesce extends EvalNode {
  constructor(readonly args: EvalNode[]) {
    super(args[0].type);
  }

  get childNodes(): EvalNode[] {
    return this.args;
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalCoalesce)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.args.length === this.args.length &&
      this.args.every((it, i) => it.isEqual(obj.args[i]))
    );
  }

  resolve(context: any) {
    for (const arg of this.args) {
      const value = arg.resolve(context);
      if (value) {
        return value;
      }
    }
  }
}

export class EvalAnd extends EvalNode {
  constructor(readonly args: EvalNode[]) {
    super(Boolean);
    args.forEach((expr) => {
      if (expr.type !== Boolean) {
        throw new DataError(
          `argument of AND must be type boolean, not type ${typeName(expr.type)}`,
        );
      }
    });
  }

  get childNodes(): EvalNode[] {
    return this.args;
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalAnd)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.args.length === this.args.length &&
      this.args.every((it, i) => it.isEqual(obj.args[i]))
    );
  }

  resolve(context: any) {
    for (const arg of this.args) {
      const value = arg.resolve(context);
      if (value === null || value === undefined) {
        return null;
      }
      if (!value) {
        return false;
      }
    }
    return true;
  }
}

export class EvalOr extends EvalNode {
  constructor(readonly args: EvalNode[]) {
    super(Boolean);
    args.forEach((expr) => {
      if (expr.type !== Boolean) {
        throw new DataError(
          `argument of OR must be type boolean, not type ${typeName(expr.type)}`,
        );
      }
    });
  }

  get childNodes(): EvalNode[] {
    return this.args;
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalOr)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.args.length === this.args.length &&
      this.args.every((it, i) => it.isEqual(obj.args[i]))
    );
  }

  resolve(context: any) {
    let response: boolean | null = false;
    for (const arg of this.args) {
      const value = arg.resolve(context);
      if (value === null || value === undefined) {
        response = null;
      }
      if (value) {
        return true;
      }
    }
    return response;
  }
}

export class EvalBetween extends EvalNode {
  constructor(
    readonly operand: EvalNode,
    readonly lower: EvalNode,
    readonly upper: EvalNode,
    readonly negate: boolean = false,
  ) {
    super(Boolean);
  }

  get childNodes(): EvalNode[] {
    return [this.lower, this.operand, this.upper];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalBetween)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.negate === this.negate &&
      obj.operand.isEqual(this.operand) &&
      obj.lower.isEqual(this.lower) &&
      obj.upper.isEqual(this.upper)
    );
  }

  resolve(context: any) {
    const operand = this.operand.resolve(context);
    if (isNull(operand)) {
      return null;
    }
    const lower = this.lower.resolve(context);
    if (isNull(lower)) {
      return null;
    }

    const upper = this.upper.resolve(context);
    if (isNull(upper)) {
      return null;
    }

    if (this.negate) {
      return !(lower <= operand && operand <= upper);
    }

    return lower <= operand && operand <= upper;
  }
}

export class EvalUnaryOp extends EvalNode {
  constructor(
    readonly operator: (a: any) => any,
    type: DType,
    readonly operand: EvalNode,
  ) {
    super(type);
  }

  get childNodes(): EvalNode[] {
    return [this.operand];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalUnaryOp)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.operand.isEqual(this.operand) &&
      obj.operator.name === this.operator.name
    );
  }

  resolve(context: any) {
    return this.operator(this.operand.resolve(context));
  }
}

export class EvalUnaryOpSafe extends EvalUnaryOp {
  constructor(
    readonly operator: (a: any) => any,
    type: DType,
    readonly operand: EvalNode,
  ) {
    super(operator, type, operand);
  }

  get childNodes(): EvalNode[] {
    return [this.operand];
  }

  resolve(context: any) {
    const operand = this.operand.resolve(context);
    if (isNull(operand)) {
      return null;
    }
    return this.operator(operand);
  }
}

export class EvalBinaryOp extends EvalNode {
  constructor(
    readonly operator: (a: any, b: any) => any,
    readonly left: EvalNode,
    readonly right: EvalNode,
    type: DType,
  ) {
    super(type);
  }

  get childNodes(): EvalNode[] {
    return [this.left, this.right];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalBinaryOp)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.left.isEqual(this.left) &&
      obj.right.isEqual(this.right) &&
      obj.operator.name === this.operator.name
    );
  }

  resolve(context: any) {
    const left = this.left.resolve(context);
    const right = this.right.resolve(context);
    if (Array.isArray(left) || Array.isArray(right)) {
      return this.operator(left, right);
    }
    if (isNull(left) || isNull(right)) {
      return null;
    }
    return this.operator(left, right);
  }
}

export class EvalAny extends EvalNode {
  constructor(
    readonly operator: (a: any, b: any) => any,
    readonly left: EvalNode,
    readonly right: EvalNode,
  ) {
    super(Boolean);
  }

  get childNodes(): EvalNode[] {
    return [this.left, this.right];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalAny)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.left.isEqual(this.left) &&
      obj.right.isEqual(this.right) &&
      obj.operator.name === this.operator.name
    );
  }

  resolve(context?: any) {
    const left = this.left.resolve(context);
    if (isNull(left)) {
      return null;
    }
    const right = this.right.resolve(context);
    if (isNull(right)) {
      return null;
    }

    if (!Array.isArray(right)) {
      throw new DataError(
        `not a list or set but ${right} ${typeName(right.type)}(${typeName(right)})`,
      );
    }

    return right.some((it) => this.operator(left, it));
  }
}

export class EvalCollection extends EvalNode {
  constructor(readonly operands: EvalNode[]) {
    super([operands[0]?.type ?? NULL]);
  }

  get childNodes() {
    return [...this.operands];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalCollection)) {
      return false;
    }
    return obj.type === this.type && isEqual(obj.operands, this.operands);
  }

  resolve(context?: any) {
    const values = this.operands.map((it) => it.resolve(context));
    return values;
  }
}

export class EvalAll extends EvalNode {
  constructor(
    readonly operator: (a: any, b: any) => any,
    readonly left: EvalNode,
    readonly right: EvalNode,
  ) {
    super(Boolean);
  }

  get childNodes() {
    return [this.left, this.right];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalAll)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.left.isEqual(this.left) &&
      obj.right.isEqual(this.right) &&
      obj.operator.name === this.operator.name
    );
  }

  resolve(context?: any) {
    const left = this.left.resolve(context);
    if (isNull(left)) {
      return null;
    }
    const right = this.right.resolve(context);
    if (isNull(right)) {
      return null;
    }

    if (!Array.isArray(right)) {
      throw new DataError(
        `not a list or set but ${typeName(right.type)}(${typeName(right)})`,
      );
    }

    return right.every((it) => this.operator(left, it));
  }
}

export class EvalWindow extends EvalNode {
  constructor(
    readonly expression: EvalAggregator,
    readonly window: EvalOver,
  ) {
    super(expression.type);
  }

  #aggregates: EvalAggregator[];
  get aggregates(): EvalAggregator[] {
    if (!this.#aggregates) {
      this.#aggregates = this.expression.traverse(
        (it) => it instanceof EvalAggregator,
      ) as EvalAggregator[];
    }
    return this.#aggregates;
  }

  get childNodes(): EvalNode[] {
    return [this.expression];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalTarget)) {
      return false;
    }
    return this.expression.isEqual(obj.expression);
  }

  resolve(context?: any) {
    return this.expression.resolve(context);
  }

  execute(data: any[], windowState?: WindowProps) {
    const allocator = new Allocator();
    const [_, aggregates] = getColumnsAndAggregates(this.expression);
    aggregates.forEach((it) => it.allocate(allocator));
    const stores = aggregates.map(() => allocator.createStore());
    aggregates.forEach((it, i) => {
      it.initialize(stores[i]);
      it.windowState = windowState;
    });

    data.forEach((context) => {
      aggregates
        .filter((it) => it.accept(context))
        .forEach((it, i) => {
          it.update(stores[i], context);
        });
    });

    aggregates.forEach((it, i) => it.finalize(stores[i]));
    return this.expression.resolve();
  }
}

export class EvalTarget extends EvalNode {
  constructor(
    readonly expression: EvalNode,
    readonly name: symbol | null,
    readonly aggregate: boolean,
    readonly visible: boolean = name !== null,
  ) {
    super(expression.type);
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalTarget)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.name === this.name &&
      obj.aggregate === this.aggregate &&
      obj.expression.isEqual(this.expression)
    );
  }

  #windows: EvalWindow[];
  get windows() {
    if (!this.#windows) {
      this.#windows = this.traverse(
        (it) => it instanceof EvalWindow,
      ) as EvalWindow[];
    }
    return this.#windows;
  }

  #hasAggregate: boolean;
  get hasAggregate() {
    if (this.#hasAggregate === undefined) {
      if (this.windows.length) {
        this.#hasAggregate = this.windows.some(
          (window) =>
            window.expression.traverse((it) => it instanceof EvalAggregator)
              .length,
        );
      } else {
        this.#hasAggregate = this.aggregate;
      }
    }

    return this.#hasAggregate;
  }

  get childNodes(): EvalNode[] {
    return [this.expression];
  }

  resolve(context?: any) {
    return this.expression.resolve(context);
  }
}

export class AttributeColumn extends EvalColumn {
  constructor(column: string, type: DType) {
    super(column, type);
  }
}

export class AttributeGetter extends AttributeColumn {
  constructor(
    column: string,
    type: DType,
    readonly getter: (context: any) => any,
  ) {
    super(column, type);
  }

  resolve(context: unknown) {
    return this.getter(context);
  }
}

const MARKER = Symbol("MARKER");
export class EvalConstantSubquery1D extends EvalNode {
  private value: any = MARKER;
  constructor(readonly subquery: EvalQuery) {
    super([subquery.columns[0].expression.type]);
  }

  get childNodes() {
    return [this.subquery];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalConstantSubquery1D)) {
      return false;
    }
    return this.subquery.isEqual(obj);
  }

  resolve(context?: any) {
    if (this.value === MARKER) {
      const [_, rows] = this.subquery.resolve(context);
      const value = rows.map((it) => it[0]);
      this.value = value.length ? value : null;
    }
    return this.value;
  }
}

export class EvalConstantSubqueryValue extends EvalNode {
  private value: any = MARKER;
  constructor(readonly subquery: EvalQuery) {
    super(subquery.columns[0].expression.type);
  }

  get childNodes() {
    return [this.subquery];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalConstantSubqueryValue)) {
      return false;
    }
    return this.subquery.isEqual(obj);
  }

  resolve(context?: any) {
    const [_, rows] = this.subquery.resolve(context);
    const values = rows.map((it) => it[0]);
    if (values.length > 1) {
      throw new ProgrammingError(
        `more than one row returned by a subquery used as an expression`,
      );
    }
    return values.length ? values[0] : null;
  }
}

export class EvalStatements extends EvalNode {
  constructor(
    readonly context: Context,
    readonly statements: Expression[],
  ) {
    super(EvalStatements);
  }

  resolve(context?: any) {
    let response: any;
    for (const statement of this.statements) {
      const expr = this.context.compiler.compileExpression(statement);
      response = expr.resolve(context);
    }
    return response;
  }

  isEqual(obj: any): boolean {
    if (
      !(obj instanceof EvalStatements) ||
      this.statements.length !== obj.statements.length
    ) {
      return false;
    }
    for (let i = 0; i < this.statements.length; i++) {
      if (!isEqual(this.statements[i], obj.statements[i])) {
        return false;
      }
    }
    return true;
  }

  get childNodes(): EvalNode[] {
    return [];
  }
}

export class EvalQuery extends EvalNode {
  constructor(
    readonly table: Table,
    readonly targets: EvalTarget[],
    readonly groupIndexes: number[],
    readonly havingIndex: number,
    readonly orderSpec: [number, "ASC" | "DESC"][],
    readonly where?: EvalNode,
    readonly limit?: number,
    readonly distinct?: boolean,
  ) {
    super(EvalQuery);
  }

  get columns() {
    return this.targets.filter((it) => it.visible);
  }

  get childNodes(): EvalNode[] {
    return [...this.targets, this.where ?? []].flat();
  }

  subQueryColumns(): Record<string, EvalColumn> {
    const columns: Record<string, EvalColumn> = {};
    this.targets
      .filter((it) => it.visible)
      .forEach((target, i) => {
        columns[target.name.description] = new AttributeGetter(
          target.name.description,
          target.type,
          (context) => {
            return context[target.name] ?? context[i];
          },
        );
      });
    return columns;
  }

  resolve(context?: any): [{ name: symbol; type: DType }[], unknown[][]] {
    const globalContext =
      context && this.table.parent?.name
        ? { [this.table.parent.name]: context }
        : undefined;
    return executeSelect(this, globalContext);
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalQuery)) {
      return false;
    }
    return (
      obj.type === this.type &&
      obj.table.name === this.table.name &&
      isEqual(
        [
          obj.targets,
          obj.groupIndexes,
          obj.havingIndex,
          obj.orderSpec,
          obj.where,
          obj.limit,
          obj.distinct,
        ],
        [
          this.targets,
          this.groupIndexes,
          this.havingIndex,
          this.orderSpec,
          this.where,
          this.limit,
          this.distinct,
        ],
      )
    );
  }
}

export class EvalPivot extends EvalQuery {
  constructor(
    query: EvalQuery,
    readonly pivots: number[],
  ) {
    super(
      query.table,
      query.targets,
      query.groupIndexes,
      query.havingIndex,
      query.orderSpec,
      query.where,
      query.limit,
      query.distinct,
    );
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalPivot)) {
      return false;
    }
    return super.isEqual(obj) && isEqual(this.pivots, obj.pivots);
  }

  resolve(context?: any): [{ name: symbol; type: DType }[], unknown[][]] {
    const [columns, rows] = super.resolve(context);
    const [col1, col2] = this.pivots;
    const otherCols = columns
      .map((_, index) => index)
      .filter((it) => !this.pivots.includes(it));
    const other = (row: any[]) => otherCols.map((it) => row[it]);
    const keys = [...new Set(rows.map((it) => it[col2]))].sort();

    // Compute the new column names and dtypes.
    const names: symbol[] = [
      Symbol(
        `${columns[col1].name.description}/${columns[col2].name.description}`,
      ),
    ];
    const dataTypes = [columns[col1].type];
    for (let i = 0; i < keys.length; i++) {
      dataTypes.push(...other(columns).map((it) => it.type as DType));
    }

    if (otherCols.length > 1) {
      names.push(
        ...[...iterableProduct(keys, other(columns))].map((arr: any[]) =>
          Symbol(`${arr[0]}/${arr[1].name.description}`),
        ),
      );
    } else {
      names.push(...keys.map((it) => Symbol(`${it}`)));
    }

    const columnDef = names.map((n, i) => ({ name: n, type: dataTypes[i] }));
    rows.sort((x: any, y: any) => {
      const a = x ? x[col1] : null;
      const b = y ? y[col1] : null;
      return a < b ? -1 : a > b ? 1 : 0;
    });

    const map = new Map<unknown, unknown[][]>();
    rows.forEach((row) => {
      const key = row[col1];
      let id = [...map.keys()].find((it) => isEqual(it, key));
      if (!id) {
        map.set(key, []);
        id = key;
      }
      map.get(id).push(row);
    });

    const pivoted: unknown[][] = [];
    for (const [field1, group] of map.entries()) {
      const row = [field1, ...group.map((it) => other(it))].flat();
      pivoted.push(
        Array.from({ length: columnDef.length }).map((_, i) => row[i] ?? null),
      );
    }

    // Populate the pivoted table.
    return [columnDef, pivoted];
  }
}

export function typedTupleToColumns(
  types: Record<string, DType>,
  additionalColumns: AttributeColumn[] = [],
  aliases: Record<string, string> = {},
): Map<string, EvalColumn> {
  const columns = new Map<string, EvalColumn>();
  for (const [name, type] of Object.entries(types)) {
    const key = aliases[name] ?? name;
    columns.set(key, new AttributeColumn(key, type));
  }
  additionalColumns.forEach((it) => {
    columns.set(it.column, it);
  });
  return columns;
}

export class EvalCreateTable extends EvalNode {
  private table: Table;
  constructor(
    readonly context: Context,
    readonly node: CreateTableExpression,
    readonly columns: Array<{ name: symbol; type: DType }>,
    readonly data?: EvalQuery,
  ) {
    super(EvalCreateTable);
    const cols = this.columns.map(
      (it) => new AttributeColumn(it.name.description, it.type),
    );
    this.table = Table.create(this.node.name, ...cols);
    context.compiler.stack.push(this.table);
    for (const column of node.columns) {
      if (column.isNotNull) {
        const expr = context.compiler.compileExpression(
          new BooleanExpression(node.parseInfo, "ISNOTNULL", [
            new ColumnExpression(node.parseInfo, column.name),
          ]),
        );
        this.table.constraints.push({
          name: `not-null`,
          expr,
          column: column.name,
        });
      }
      if (column.check) {
        const expr = context.compiler.compileExpression(column.check);
        if (expr.type !== Boolean) {
          throw new CompilationError(
            `argument of CHECK must be type boolean, not type ${typeName(expr.type)}`,
            node,
          );
        }
        this.table.constraints.push({
          name: `${this.table.name}_${column.name}_check`,
          expr,
          column: column.name,
        });
      }
    }
    for (const constraint of node.constraints) {
      if (constraint instanceof CheckConstraint) {
        const expr = context.compiler.compileExpression(constraint.expression);
        if (expr.type !== Boolean) {
          throw new CompilationError(
            `argument of CHECK must be type boolean, not type ${typeName(expr.type)}`,
            node,
          );
        }
        this.table.constraints.push({
          name: constraint.constraintName(),
          expr,
        });
      }
    }
    context.compiler.stack.pop();
  }

  get childNodes(): EvalNode[] {
    return [];
  }

  resolve(context?: any): [{ name: symbol; type: DType }[], unknown[][]] {
    const data: unknown[][] = [];
    if (this.data) {
      const [_, queryData] = this.data.resolve(context);
      this.table = this.table.data(queryData);
      data.push(...queryData);
    }
    this.context.withTables(this.table);
    return [this.columns, data];
  }

  isEqual(obj: any): boolean {
    if (!(obj instanceof EvalCreateTable)) {
      return false;
    }
    return (
      this.node.name === obj.node.name &&
      this.node.using == obj.node.using &&
      isEqual(this.data, obj.data)
    );
  }
}
