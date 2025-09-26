/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  AllExpression,
  AnyExpression,
  AsteriskExpression,
  AttributeExpression,
  BooleanExpression,
  ColumnExpression,
  Expression,
  FromClause,
  FunctionExpression,
  GroupByExpression,
  ListExpression,
  LiteralExpression,
  OrderByExpression,
  OrderExpression,
  PivotByExpression,
  PlaceHolderExpression,
  Query,
  QueryExpression,
  SubscriptExpression,
  SubQueryExpression,
  TableExpression,
  TargetExpression,
  CreateTableExpression,
  InsertExpression,
  WildcardExpression,
  JoinExpression,
  OverExpression,
  CaseExpression,
  CollectionExpression,
  CastExpression,
  StatementExpression,
  UpdateTableExpression,
} from "..";
import {
  CompilationError,
  InternalError,
  NotSupportedError,
  ProgrammingError,
} from "../errors";
import {
  EvalAggregator,
  EvalAll,
  EvalAnd,
  EvalAny,
  EvalCase,
  EvalCoalesce,
  EvalCollection,
  EvalColumn,
  EvalConstant,
  EvalConstantSubquery1D,
  EvalConstantSubqueryValue,
  EvalFunction,
  EvalGetItem,
  EvalGetter,
  EvalInsert,
  EvalOr,
  EvalOver,
  EvalPivot,
  EvalQuery,
  EvalTarget,
  EvalWindow,
  EvalCreateTable,
  findFunction,
  findOperator,
  EvalStatements,
  EvalUpdateTable,
  findColumnFunction,
  AttributeColumn,
} from "./nodes";
import { SubQueryTable, Table } from "./models";
import { Context } from "./context";
import { ConstantValue, isSameType, NULL, Operation, structureFor, typeFor } from "./types";
import { ASTERISK, DType, EvalNode, isNull, typeName } from "./types";
import "./query_env";

const NULLTABLE = Object.freeze(
  Table.create("<null>").data(
    Object.freeze([Object.freeze({})]) as Array<Record<string, unknown>>,
  ),
);

export type CompilerOptions = {
  supportImplicitGroupBy: boolean;
  defaultTableName: string;
};

const COMPILER_OPTIONS: CompilerOptions = Object.freeze({
  supportImplicitGroupBy: true,
  defaultTableName: "peql",
});

export class Compiler {
  parameters: Record<string, ConstantValue> = {};
  stack: Array<Table> = [];
  queries: Array<Query> = [];
  modes: Array<string> = [];
  expressions: Array<Expression> = [];
  columnFunctions: Array<Array<Operation>> = [];
  readonly options: CompilerOptions;

  constructor(
    readonly context: Context,
    options: Partial<CompilerOptions> = {},
  ) {
    this.context.compiler = this;
    this.options = { ...COMPILER_OPTIONS, ...options };
  }

  get mode() {
    return this.modes[this.modes.length - 1];
  }

  get table(): Table {
    return this.stack[this.stack.length - 1] ?? NULLTABLE;
  }

  set table(value: Table | string) {
    if (typeof value === "string") {
      const table = this.context.tables.get(value);
      if (!table) {
        throw new CompilationError(`relation "${value}" does not exist`);
      }
      this.stack[this.stack.length - 1] = table;
      return;
    }
    if (!value) {
      throw new CompilationError(
        `relation "${this.options.defaultTableName}" does not exist`,
      );
    }
    this.stack[this.stack.length - 1] = value;
  }

  compile(
    query: Expression,
    parameters?: Record<string, ConstantValue>,
    options?: Record<string, ConstantValue>,
  ) {
    this.stack = [];
    this.queries = [];
    if (
      this.options.defaultTableName &&
      this.context.tables.get(this.options.defaultTableName)
    ) {
      this.stack.push(this.context.tables.get(this.options.defaultTableName));
    } else {
      this.stack.push(NULLTABLE);
    }

    if (parameters) {
      if (Array.isArray(parameters)) {
        parameters.forEach((value, index) => {
          this.parameters[index.toString()] = value;
        });
      } else {
        this.parameters = { ...parameters };
      }
    }
    const placeholders = query
      .children()
      .filter((it) => it instanceof PlaceHolderExpression);
    if (placeholders.length) {
      if (placeholders.every((it) => it.name)) {
        if (Array.isArray(parameters)) {
          throw new TypeError(
            `query parameters should be a mapping when using named placeholders`,
          );
        } else {
          const params = Object.keys(parameters);
          const missing = placeholders
            .map((it) => it.name)
            .filter((it) => !params.includes(it));
          if (missing.length) {
            throw new ProgrammingError(`query parameter missing: ${missing}`);
          }
        }
      } else if (placeholders.every((it) => !it.name)) {
        if (!Array.isArray(parameters)) {
          throw new TypeError(
            `query parameters should be a sequence when using positional placeholders`,
          );
        }
        if (placeholders.length !== parameters.length) {
          throw new ProgrammingError(
            `the query has ${placeholders.length} placeholders but ${parameters.length} parameters were passed`,
          );
        }
        placeholders
          .sort((a, b) => a.parseInfo.pos - b.parseInfo.pos)
          .forEach((entry, index) => (entry.name = index.toString()));
      } else {
        throw new ProgrammingError(
          "positional and named parameters cannot be mixed",
        );
      }
    }
    const expr = this.compileExpression(query, options);
    this.stack.pop();
    return expr;
  }

  compileQuery(
    node: Query,
    options?: Record<string, ConstantValue>,
  ): EvalQuery {
    if (!this.table) {
      throw new CompilationError(
        `No active table for query: ${this.options.defaultTableName}`,
        node,
      );
    }
    this.stack.push(this.table);
    this.queries.push(node);

    for (const [key, subQuery] of Object.entries(node.commonTableExpressions)) {
      this.context.tables.set(
        key,
        new SubQueryTable(this.compileQuery(subQuery), key),
      );
    }
    const fromExpr = this.compileFrom(node.from, options);
    this.compileJoins(node.joins);
    const targets = this.compileTargets(node.select.targets);

    let where = this.compileExpression(node.where);
    if (where && where.type !== Boolean && where.type !== NULL) {
      throw new CompilationError(
        `argument of WHERE must be type boolean, not type ${typeName(where.type)}`,
        node.where,
      );
    }

    // Check that the FROM clause does not contain aggregates. This
    // should never trigger if the compilation environment does not
    // contain any aggregate.
    if (where && isAggregate(where)) {
      throw new CompilationError(
        `aggregates are not allowed in WHERE clause: ${node.where}`,
        node.where,
      );
    }

    // Combine FROM and WHERE clauses
    if (fromExpr) {
      where = isNull(where) ? fromExpr : new EvalAnd([fromExpr, where]);
    }

    //Process the GROUP-BY clause.
    const [groupByTargets, groupIndexes, havingIndex] = this.compileGroupBy(
      targets,
      node.groupBy,
    );

    targets.push(...groupByTargets);

    //  Process the ORDER-BY clause.
    const [orderByTargets, orderSpec] = this.compileOrderBy(
      targets,
      node.orderBy,
    );
    targets.push(...orderByTargets);

    const isGroupBy =
      targets.some((it) => it.hasAggregate) || groupIndexes.length > 0;

    if (isGroupBy) {
      const nonAggrIndexes: number[] = [];
      targets.forEach((it, index) => {
        if (!it.aggregate) {
          nonAggrIndexes.push(index);
        }
        if (!it.visible && !it.aggregate && !groupIndexes.includes(index)) {
          groupIndexes.push(index);
        }
      });

      const missing = nonAggrIndexes.filter((it) => !groupIndexes.includes(it));
      if (missing.length) {
        if (this.options.supportImplicitGroupBy) {
          groupIndexes.push(...missing);
        } else {
          throw new CompilationError(
            `column(s) ${missing.map((index) => (targets[index].name ? `"${this.table.name}"."${targets[index]}"` : targets[index])).join(",")} must appear in the GROUP BY clause or be used in an aggregate function`,
          );
        }
      }
    }

    let compiledQuery: EvalQuery | EvalPivot = new EvalQuery(
      this.table,
      targets,
      groupIndexes,
      havingIndex,
      orderSpec,
      where,
      node.limit,
      node.select.distinct,
    );

    const pivots = this.compilePivotBy(targets, groupIndexes, node.pivotBy);
    if (pivots.length) {
      compiledQuery = new EvalPivot(compiledQuery, pivots);
    }
    this.stack.pop();
    this.queries.pop();
    return compiledQuery;
  }

  compileAsteriskExpression(expression: AsteriskExpression) {
    return this.table.wildcardColumns.map((name) => {
      const type = this.table.columns.get(name)!.type;
      if (structureFor(type)) {
        return new TargetExpression(
          expression.parseInfo,
          new WildcardExpression(expression.parseInfo, name),
        );
      }
      return new TargetExpression(
        expression.parseInfo,
        new AttributeExpression(
          expression.parseInfo,
          new ColumnExpression(expression.parseInfo, this.table.name),
          name,
        ),
        name,
      );
    });
  }

  expandTargets(targets: Array<TargetExpression>) {
    const expandTargets: TargetExpression[] = [];
    for (const target of targets) {
      if (target.expression instanceof AsteriskExpression) {
        expandTargets.push(
          ...this.expandTargets(
            this.compileAsteriskExpression(target.expression),
          ),
        );
      } else if (target.expression instanceof WildcardExpression) {
        expandTargets.push(...this.compileWildCard(target.expression));
      } else {
        expandTargets.push(target);
      }
    }
    return expandTargets;
  }

  compileTargets(columns: Array<TargetExpression>) {
    const targets: TargetExpression[] = this.expandTargets(columns);
    const compiledTargets: EvalTarget[] = [];

    for (const [i, target] of targets.entries()) {
      let expr = this.compileExpression(target.expression);
      if (expr instanceof EvalQuery) {
        if (expr.columns.length > 1) {
          throw new CompilationError(
            `subquery must return only one column\n${targets[i]}`,
            target.expression,
          );
        }
        expr = new EvalConstantSubqueryValue(expr);
      }
      compiledTargets.push(
        new EvalTarget(
          expr,
          target.name ? Symbol(target.name) : null,
          isAggregate(expr),
        ),
      );
      const [columns, aggregates] = getColumnsAndAggregates(expr);
      const windowFunctions = [expr, ...expr.childNodes].filter(
        (it) => it instanceof EvalWindow,
      );
      if (windowFunctions.length) {
        windowFunctions.forEach((agg) => {
          if (agg.aggregates.length > 1) {
            throw new CompilationError(
              `aggregate function calls cannot be nested: ${target}`,
              target,
            );
          }
        });
      } else {
        if (columns.length && aggregates.length) {
          // throw new CompilationError(
          //   `mixed aggregates and non-aggregates(${columns.map((it) => it.column).join(",")}) are not allowed`,
          // );
        }
        aggregates.forEach((agg) => {
          agg.childNodes.forEach((child) => {
            if (isAggregate(child)) {
              throw new CompilationError(
                `aggregate function calls cannot be nested: ${target}`,
                target,
              );
            }
          });
        });
      }
    }

    const windowTargets: EvalWindow[] = compiledTargets
      .map((target) => {
        return target.traverse(
          (it) => it instanceof EvalWindow,
        ) as EvalWindow[];
      })
      .flat();
    windowTargets.forEach(({ window }) => {
      if (window.expression.orderBy) {
        const [orderTargets, orderSpec] = this.compileOrderBy(
          compiledTargets,
          window.expression.orderBy,
        );
        compiledTargets.push(...orderTargets);
        window.orderBy = orderSpec;
        if (
          window.frame.type === "RANGE" &&
          (window.frame.preceding !== Infinity ||
            (window.frame.following > 0 && window.frame.following !== Infinity))
        ) {
          if (orderSpec.length !== 1) {
            throw new CompilationError(
              `RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column`,
              window.expression.orderBy,
            );
          }
          const type = compiledTargets[orderSpec[0][0]].type;
          if (type === String) {
            throw new CompilationError(
              `RANGE with offset PRECEDING/FOLLOWING is not supported for column type text`,
              window.expression.orderBy,
            );
          }
        }
      }

      if (
        window.frame.type === "RANGE" &&
        window.expression.orderBy?.columns?.length !== 1
      ) {
        throw new CompilationError(
          `RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column`,
          window.expression.orderBy,
        );
      }

      if (window.expression.partitionBy) {
        const [partitionTargets, partitions] = this.compilePartitionBy(
          compiledTargets,
          [window.expression.partitionBy],
        );
        compiledTargets.push(...partitionTargets);
        window.partitionBy = partitions;
      }
    });
    return compiledTargets;
  }

  compileOverExpression(node?: OverExpression): EvalOver | null {
    if (!node) {
      return null;
    }

    let overExpr: OverExpression = node.copy();
    if (node.name) {
      overExpr = this.queries[this.queries.length - 1].windows[node.name];
      if (!overExpr) {
        throw new CompilationError(
          `Invalid window: ${node.name} specified in ${node}`,
          overExpr,
        );
      }
      if (node.partitionBy) {
        overExpr = overExpr.copy({ partitionBy: node.partitionBy });
      }
      if (node.orderBy) {
        overExpr = overExpr.copy({ orderBy: node.orderBy });
      }
      if (node.frame) {
        overExpr = overExpr.copy({ frame: node.frame });
      }
    }

    return {
      expression: overExpr,
      frame: overExpr.frame,
    };
  }

  isEquiJoin(expr: Expression, columns: Array<Expression>): boolean {
    if (expr instanceof BooleanExpression) {
      for (const arg of expr.args) {
        if (!this.isEquiJoin(arg, columns)) {
          return false;
        }
      }
      return true;
    } else if (expr instanceof AttributeExpression) {
      if (typeof expr.name !== "string") {
        return true;
      }
      if (expr.operand instanceof ColumnExpression) {
        columns.push(expr);
        return true;
      }
      return false;
    } else if (expr instanceof ColumnExpression) {
      columns.push(expr);
      return true;
    } else if (expr instanceof CastExpression) {
      return this.isEquiJoin(expr.expr, columns);
    } else if (expr instanceof LiteralExpression) {
      return true;
    } else if (expr instanceof FunctionExpression) {
      for (const arg of expr.args) {
        if (!this.isEquiJoin(arg, columns)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  compileJoins(nodes: JoinExpression[]) {
    if (!nodes.length) {
      return;
    }

    for (const joinExpr of nodes) {
      let rightKey = "";
      const joinTable =
        joinExpr.table instanceof TargetExpression
          ? joinExpr.table.expression
          : joinExpr.table;
      let table: Table | null;

      if (joinTable instanceof TableExpression) {
        table = this.context.tables.get(joinTable.table);
        rightKey =
          joinExpr.table instanceof TargetExpression
            ? joinExpr.table.name
            : joinTable.name;
      } else if (joinTable instanceof SubQueryExpression) {
        if (joinExpr.table instanceof TargetExpression) {
          rightKey = joinExpr.table.as;
        } else {
          throw new CompilationError(
            `Subqery expression missing alias: ${joinTable}`,
            joinExpr.table,
          );
        }
        table = new SubQueryTable(this.compileQuery(joinTable.query), rightKey);
      }

      if (table) {
        this.table = this.table.copy();
        this.table.columns.set(
          rightKey,
          new EvalColumn(rightKey, table.toStructure()),
        );
        this.table.joins.set(rightKey, table);
      } else {
        throw new CompilationError(
          `relation "${joinTable}" does not exists`,
          joinTable,
        );
      }

      const results: unknown[] = [];
      const expr = this.compileExpression(joinExpr.condition);

      const columns: Array<AttributeExpression | ColumnExpression> = [];

      const matchedRightRows = new Set<any>();
      const matchedLeftRows = new Set<any>();

      const tableName = this.table.name;
      const joinedTable = this.table;

      const leftColumns: Array<EvalNode> = [];
      const rightColumns: Array<EvalNode> = [];

      if (this.isEquiJoin(joinExpr.condition, columns)) {
        for (const column of columns) {
          if (column instanceof AttributeExpression) {
            if (column.operand instanceof ColumnExpression) {
              if (
                column.operand.column === table.name ||
                column.operand.column === rightKey
              ) {
                rightColumns.push(this.compileExpression(column));
              } else {
                leftColumns.push(this.compileExpression(column));
              }
            }
          }
        }
      }

      if (leftColumns.length === rightColumns.length) {
        const dataLoader = () => {
          const left: any[] = joinedTable.rows;
          const right: unknown[] = [...table];
          const rightMap = new Map<string, Array<unknown>>();
          for (const rightRow of right) {
            const key = rightColumns
              .map((it) =>
                it.resolve({
                  [rightKey]: rightRow,
                }),
              )
              .join(":");
            if (!rightMap.has(key)) rightMap.set(key, []);
            rightMap.get(key).push(rightRow);
          }

          if (joinExpr.type === "CROSS") {
            for (const leftRow of left) {
              const key = leftColumns
                .map((it) => it.resolve(leftRow))
                .join(":");
              const matches = rightMap.get(key) || [];
              for (const rightRow of matches) {
                const data: Record<string, unknown> = {
                  ...leftRow,
                  [tableName]: leftRow,
                  [rightKey]: rightRow,
                };
                if (expr.resolve(data)) {
                  results.push({ ...leftRow, [rightKey]: rightRow });
                }
              }
            }
          } else {
            for (const leftRow of left) {
              let matched = false;
              const key = leftColumns
                .map((it) => it.resolve(leftRow))
                .join(":");
              const matches = rightMap.get(key) || [];
              for (const rightRow of matches) {
                const data: Record<string, unknown> = {
                  ...leftRow,
                  [tableName]: leftRow,
                  [rightKey]: rightRow,
                };
                if (expr.resolve(data)) {
                  matched = true;
                  matchedRightRows.add(rightRow);
                  matchedLeftRows.add(leftRow);
                  results.push({ ...leftRow, [rightKey]: rightRow });
                }
              }

              if (
                (!matched && joinExpr.type === "LEFT") ||
                joinExpr.type === "FULL"
              ) {
                results.push({ ...leftRow });
              }

              if (joinExpr.type === "RIGHT" || joinExpr.type === "FULL") {
                for (const rightRow of right) {
                  if (!matchedRightRows.has(rightRow)) {
                    results.push({ [rightKey]: rightRow });
                  }
                }
              }
            }

            // returns only left rows that did not match any right row
            if (joinExpr.type === "ANTI") {
              return left.filter((leftRow) => !matchedLeftRows.has(leftRow));
            }
          }
          return results;
        };

        this.table = this.table.data(dataLoader);
      } else {
        const dataLoader = () => {
          const left: any[] = joinedTable.rows;
          const right: unknown[] = [...table];

          if (joinExpr.type === "CROSS") {
            for (const leftRow of left) {
              for (const rightRow of right) {
                const data: Record<string, unknown> = {
                  ...leftRow,
                  [tableName]: leftRow,
                  [rightKey]: rightRow,
                };
                if (expr.resolve(data)) {
                  results.push({ ...leftRow, [rightKey]: rightRow });
                }
              }
            }
          } else {
            for (const leftRow of left) {
              let matched = false;
              for (const rightRow of right) {
                const data: Record<string, unknown> = {
                  ...leftRow,
                  [tableName]: leftRow,
                  [rightKey]: rightRow,
                };
                if (expr.resolve(data)) {
                  matched = true;
                  matchedRightRows.add(rightRow);
                  matchedLeftRows.add(leftRow);
                  results.push({ ...leftRow, [rightKey]: rightRow });
                }
              }

              if (
                (!matched && joinExpr.type === "LEFT") ||
                joinExpr.type === "FULL"
              ) {
                results.push({ ...leftRow });
              }

              if (joinExpr.type === "RIGHT" || joinExpr.type === "FULL") {
                for (const rightRow of right) {
                  if (!matchedRightRows.has(rightRow)) {
                    results.push({ [rightKey]: rightRow });
                  }
                }
              }
            }

            // returns only left rows that did not match any right row
            if (joinExpr.type === "ANTI") {
              return left.filter((leftRow) => !matchedLeftRows.has(leftRow));
            }
          }
          return results;
        };

        this.table = this.table.data(dataLoader);
      }
    }
  }

  compileFrom(
    node?: FromClause,
    _options?: Record<string, ConstantValue>,
  ) {
    if (!node) {
      return null;
    }
    const parent = this.table;
    if (node.from instanceof SubQueryExpression) {
      const query = this.compileQuery(node.from.query);
      if (query instanceof EvalQuery) {
        this.table = new SubQueryTable(query);
        this.table.props.parent = parent;
        // if (current.name) {
        //   this.table.columns.set(
        //     current.name,
        //     new EvalColumn(current.name, current.toStructure())
        //   )
        // }
        return null;
      } else {
        throw new CompilationError(
          `Unsupported sub query: ${typeName(query)}`,
          query,
        );
      }
    }
    // Table Reference
    if (node.from instanceof TableExpression) {
      this.table = node.from.table;
      if (!this.table) {
        throw new CompilationError(
          `table "${node.from.table}" does not exist`,
          node.from,
        );
      }

      if (node.from.alias) {
        this.table = this.table.copy({
          name: node.from.alias,
          props: { parent },
        });
        this.context.tables.set(node.from.alias, this.table);
      }
    }

    // FROM expression
    if (node.from) {
      const expr = this.compileExpression(node.from);
      // Check that the FROM clause does not contain aggregates.
      if (expr && isAggregate(expr)) {
        throw new CompilationError(
          "aggregates are not allowed in FROM clause",
          node.from,
        );
      }
      return expr;
    }
  }

  compileWildCard(node: WildcardExpression): TargetExpression[] {
    // d.* d.age
    const column = this.table.getColumn(node.column);
    if (!column) {
      if (node.column === this.table.name) {
        return this.compileAsteriskExpression(
          new AsteriskExpression(node.parseInfo),
        );
      }
      throw new CompilationError(
        `column "${node.column}" not found in table "${this.table.name}"`,
        node,
      );
    }

    const dType = structureFor(column.type);
    if (!dType) {
      throw new CompilationError(
        `wildcard column is not structured: ${typeName(dType ?? column.type)}`,
        node,
      );
    }

    const columns = dType.wildcardColumns.length
      ? dType.wildcardColumns
      : [...dType.columns.keys()];

    return columns.map((column) => {
      return new TargetExpression(
        node.parseInfo,
        new AttributeExpression(
          node.parseInfo,
          new ColumnExpression(node.parseInfo, node.column),
          column,
        ),
        `${node.column}.${column}`,
      );
    });
  }

  compileExpression(
    node?: Expression,
    options?: Record<string, ConstantValue>,
  ): EvalNode | null {
    if (!node) {
      return null;
    }
    this.expressions.push(node);
    try {
      const value = this._compileExpression(node, options);
      if (value) {
        value.expr = node;
      }
      this.expressions.pop();
      return value;
    } catch (e) {
      this.expressions.pop();
      throw e;
    }
  }

  _compileExpression(
    node?: Expression,
    options?: Record<string, ConstantValue>,
  ): EvalNode | null {
    if (!node) {
      return null;
    }
    if (node instanceof Query) {
      return this.compileQuery(node, options);
    } else if (node instanceof CreateTableExpression) {
      return this.compileCreateTable(node);
    } else if (node instanceof UpdateTableExpression) {
      return this.compileUpdateTable(node);
    } else if (node instanceof LiteralExpression) {
      return new EvalConstant(node.value);
    } else if (node instanceof ListExpression) {
      return new EvalConstant(node.value);
    } else if (node instanceof PlaceHolderExpression) {
      return new EvalConstant(this.parameters[node.name]);
    } else if (node instanceof ColumnExpression) {
      const column = this.table.getColumn(node.name);
      if (column) {
        return column;
      } else {
        const columnFns = this.columnFunctions[this.columnFunctions.length -1];
        if (columnFns && columnFns.length) {
          for(const op of columnFns) {
            if (op.columns[node.name.toLowerCase()]) {
              return new AttributeColumn(node.name, op.columns[node.name.toLowerCase()]);
            }
          }
        }
        throw new CompilationError(
          `column "${node.column}" not found in table "${this.table.name}"`,
          node,
        );
      }
    } else if (node instanceof BooleanExpression) {
      const args = node.args.map((it) => this.compileExpression(it));
      if (["AND", "OR"].includes(node.op)) {
        if (args.some((it) => it.type !== Boolean)) {
          const operator = findOperator(node.op, args);
          if (operator) {
            if (
              args.length == 2 &&
              args[0] instanceof EvalConstant &&
              args[1] instanceof EvalConstant
            ) {
              return new EvalConstant(
                operator.create(...args).resolve(),
                operator.output,
              );
            }
            return operator.create(...args);
          }
        }
      }
      if (node.op === "AND") {
        return new EvalAnd(args);
      } else if (node.op === "OR") {
        return new EvalOr(args);
      } else {
        const isIn = ["IN", "NOTIN"].includes(node.op);
        args.forEach((arg, i) => {
          let query: EvalQuery | null = null;
          if (arg instanceof EvalQuery) {
            query = arg;
          } else if (arg instanceof EvalCollection) {
            if (isIn) {
              const queries = arg.operands.filter(
                (it) => it instanceof EvalQuery,
              );
              if (queries.length > 0) {
                if (queries.length == 1) {
                  query = queries[0];
                } else {
                  throw new CompilationError(
                    `syntax error at or near: ${node.args[i]}`,
                    node.args[i],
                  );
                }
              }
            }
          }

          if (query) {
            if (query.targets.filter((it) => it.name).length !== 1) {
              throw new CompilationError(
                `subquery must return only one column\n${node.args[i]}`,
                node.args[i],
              );
            }

            if (isIn) {
              args[i] = new EvalConstantSubquery1D(query);
            } else {
              args[i] = new EvalConstantSubqueryValue(query);
            }
          }
        });

        if (node.op === "NOT" && args.length == 1) {
          if (args[0].type !== Boolean) {
            if (args.some((it) => it.type === NULL)) {
              return new EvalConstant(null, NULL);
            }
            throw new CompilationError(
              `argument of NOT must be type boolean, not type ${typeName(args[0].type)}\n${node.args[0]}`,
              node.args[0],
            );
          }
        }

        let operator = findOperator(node.op, args);
        if (operator) {
          if (
            args.length == 2 &&
            args[0] instanceof EvalConstant &&
            args[1] instanceof EvalConstant
          ) {
            return new EvalConstant(
              operator.create(...args).resolve(),
              operator.output,
            );
          }
          return operator.create(...args);
        }

        if (args.length == 2) {
          let [left, right] = args;
          while (left && right) {
            operator = findOperator(node.op, [left, right]);
            if (operator) {
              if (
                left instanceof EvalConstant &&
                right instanceof EvalConstant
              ) {
                return new EvalConstant(
                  operator.create(...args).resolve(),
                  operator.output,
                );
              }
              return operator.create(left, right);
            }

            if (left.type === Object && right.type !== Object) {
              left = findFunction(typeName(right.type), [left])?.create(
                this.context,
                left,
              );
              continue;
            }

            if (right.type === Object && left.type !== Object) {
              right = findFunction(typeName(left.type), [left])?.create(
                this.context,
                right,
              );
              continue;
            }

            break;
          }
        }

        if (args.length == 2) {
          const [left, right] = args;
          if (left.type == NULL || right.type == NULL) {
            return new EvalConstant(null);
          }
        }

        if (args.length == 1) {
          if (args[0].type === NULL) {
            return new EvalConstant(null);
          }
          throw new NotSupportedError(
            `Unsupported operator ${node.op}(${node.args[0]}::${typeName(args[0].type)})`,
          );
        }

        if (args.some((it) => it.type === NULL)) {
          return new EvalConstant(null, Boolean);
        }
        throw new NotSupportedError(
          `Unsupported operator(${node.op}): (${args.map((it) => typeName(it?.type)).join(` ${node.op} `)})`,
        );
      }
    } else if (node instanceof AnyExpression || node instanceof AllExpression) {
      let right = this.compileExpression(node.right);
      if (right instanceof EvalQuery) {
        if (right.targets.filter((it) => it.name).length !== 1) {
          throw new CompilationError(
            "subquery has too many columns",
            node.right,
          );
        }
        right = new EvalConstantSubquery1D(right);
      }
      let rightType = right.type;
      if (!(Array.isArray(rightType) || right.type === Set)) {
        throw new CompilationError(
          `not a list or set but ${typeName(right)}`,
          node.right,
        );
      }
      if (Array.isArray(rightType)) {
        rightType = rightType[0];
      }
      const left = this.compileExpression(node.left);

      const operator = findOperator(node.op, [left, rightType]);
      if (operator) {
        return node instanceof AnyExpression
          ? new EvalAny(operator.operator, left, right)
          : new EvalAll(operator.operator, left, right);
      } else {
        throw new NotSupportedError(
          `Unsupported operator: (${typeName(left)} ${node.op} ${typeName(rightType)})`,
        );
      }
    } else if (node instanceof FunctionExpression) {
      return this.compileFunction(node);
    } else if (node instanceof QueryExpression) {
      return this.compileExpression(node.expression);
    } else if (node instanceof AsteriskExpression) {
      return new EvalConstant(null, ASTERISK);
    } else if (node instanceof AttributeExpression) {
      return this.compileAttribute(node);
    } else if (node instanceof SubscriptExpression) {
      return this.compileSubscript(node);
    } else if (node instanceof OrderExpression) {
      return this.compileExpression(node.column);
    } else if (node instanceof InsertExpression) {
      return this.compileInsert(node);
    } else if (node instanceof CollectionExpression) {
      const operands = node.values.map((it) => this.compileExpression(it));
      const invalid = operands
        .map((it, index) =>
          operands[0].type !== NULL && it.type !== operands[0].type
            ? [`${typeName(it.type)}("${node.values[index]}")`]
            : [],
        )
        .flat();
      if (invalid.length) {
        throw new CompilationError(
          `invalid input syntax for type(${typeName(operands[0].type)}): ${invalid.join(", ")}`,
          node,
        );
      }
      return new EvalCollection(operands);
    } else if (node instanceof CaseExpression) {
      const operands = node.conditions.map((it) => ({
        when: this.compileExpression(it.when),
        then: this.compileExpression(it.then),
      }));
      if (!operands.length) {
        throw new CompilationError(`Invalid case expression\n${node}`, node);
      }
      const fallback = node.fallback
        ? this.compileExpression(node.fallback)
        : undefined;
      const type = fallback?.type ?? operands[0].then.type;
      operands.forEach((op, index) => {
        if (op.when.type !== Boolean) {
          throw new CompilationError(
            `invalid input syntax for type boolean\n"${node.conditions[index].when}"`,
            node,
          );
        } else if (op.then.type !== type) {
          throw new CompilationError(
            `invalid input syntax for type ${typeName(type)}\n"${node.conditions[index].then}"`,
            node,
          );
        }
      });
      return new EvalCase(operands, fallback);
    } else if (node instanceof TableExpression) {
      return null;
    } else if (node instanceof CastExpression) {
      return this.compileExpression(
        new FunctionExpression(node.parseInfo, node.type, [node.expr]),
      );
    } else if (node instanceof StatementExpression) {
      return new EvalStatements(this.context, node.statements);
    } else if (node instanceof TargetExpression) {
      const expr = this.compileExpression(node.expression);
      return new EvalTarget(
        expr,
        node.name ? Symbol(node.name) : null,
        isAggregate(expr),
      );
    }

    throw new NotSupportedError(
      `Expression ${node.constructor.name} not supported: ${node}`,
    );
  }

  compileFunction(node: FunctionExpression) {
    this.columnFunctions.push(findColumnFunction(node.name));
    const operands = node.args.map((it) => this.compileExpression(it));
    this.columnFunctions.pop();
    if (node.name === "coalesce") {
      if (operands.some((it) => it.type !== operands[0].type)) {
        throw new CompilationError(
          `coalesce() function arguments must have uniform type, found: ${operands.map((it) => typeName(it.type)).join(", ")}`,
          node,
        );
      }
      return new EvalCoalesce(operands);
    }

    const fn = findFunction(node.name, operands);
    if (!fn) {
      if (operands.some((it) => it.type === NULL)) {
        return new EvalConstant(null);
      }
      throw new CompilationError(
        `no function matches "${node.name}(${operands.map((it) => typeName(it.type)).join(", ")})" name and argument type`,
        node,
      );
    }

    // Replace ``meta(key)`` with ``meta[key]``.
    if (node.name === "meta") {
      return this.compileExpression(
        new FunctionExpression(node.parseInfo, "getitem", [
          new ColumnExpression(node.parseInfo, "meta"),
          node.args[0],
        ]),
      );
    }

    //  Replace ``entry_meta(key)`` with ``entry.meta[key]``.
    if (node.name === "entry_meta") {
      return this.compileExpression(
        new FunctionExpression(node.parseInfo, "getitem", [
          new AttributeExpression(
            node.parseInfo,
            new ColumnExpression(node.parseInfo, "meta"),
            "entry",
          ),
          node.args[0],
        ]),
      );
    }

    // Replace ``any_meta(key)`` with ``getitem(meta, key, entry.meta[key])``.
    if (node.name === "any_meta") {
      return this.compileExpression(
        new FunctionExpression(node.parseInfo, "getitem", [
          new ColumnExpression(node.parseInfo, "meta"),
          node.args[0],
          new FunctionExpression(node.parseInfo, "getitem", [
            new AttributeExpression(
              node.parseInfo,
              new ColumnExpression(node.parseInfo, "meta"),
              "entry",
            ),
            node.args[0],
          ]),
        ]),
      );
    }

    // Replace ``has_account(regexp)`` with ``('(?i)' + regexp) ~ any (accounts)``.
    if (node.name === "has_account") {
      return this.compileExpression(
        new AnyExpression(
          node.parseInfo,
          "?~*",
          node.args[0],
          new ColumnExpression(node.parseInfo, "accounts"),
        ),
      );
    }

    const execFn = fn.create(this.context, ...operands) as EvalFunction;
    const window = this.compileOverExpression(node.window);

    if (operands.every((it) => it instanceof EvalConstant) && execFn.pure) {
      return new EvalConstant(execFn.resolve(), execFn.type);
    }

    if (node.filter) {
      if (execFn instanceof EvalAggregator) {
        const expr = this.compileExpression(node.filter);
        if (isAggregate(expr)) {
          throw new CompilationError(
            `FILTER expressions may not be aggregates: ${node.filter}`,
            node.filter,
          );
        }
        execFn.filter = expr;
      } else {
        throw new CompilationError(
          `Filter functions can only operate on aggregate functions`,
          node.filter,
        );
      }
    }

    if (node.distinct) {
      if (execFn instanceof EvalAggregator) {
        execFn.distinct = true;
      } else {
        throw new CompilationError(
          `DISTINCT specified, but ${node.name} is not an aggregate function`,
          node,
        );
      }
    }

    if (window) {
      if (execFn instanceof EvalAggregator) {
        return new EvalWindow(execFn, window);
      } else {
        throw new CompilationError(
          `window functions may only be used in aggregate functions: ${node}`,
          node,
        );
      }
    }
    return execFn;
  }

  compilePartitionBy(
    targets: EvalTarget[],
    expressions: Expression[],
  ): [EvalTarget[], number[]] {
    const newTargets: EvalTarget[] = [];
    const targetExpressions = targets.map((it) => it.expression);
    const partitions: number[] = [];

    const targetsNameMap: any = targets.reduce((acc: any, target, i) => {
      if (target.name) {
        acc[target.name] = i;
        acc[target.name.description] = i;
      }
      return acc;
    }, {});

    const size = Object.getOwnPropertySymbols(targetsNameMap).length;
    expressions.forEach((expr) => {
      let index: number | null;

      const column = numberOrExpression(expr);
      if (typeof column === "number") {
        index = column - 1;
        if (index >= size || index < 0) {
          throw new CompilationError(
            `invalid PARTITION-BY column index ${column}`,
            expr,
          );
        }
      } else {
        if (column instanceof ColumnExpression) {
          index = targetsNameMap[column.name];
        }
        if (isNull(index)) {
          const expr = this.compileExpression(column);
          index = targetExpressions.findIndex((it) => it.isEqual(expr));
          if (index === -1) {
            index = targets.length + newTargets.length;
            newTargets.push(new EvalTarget(expr, null, isAggregate(expr)));
          }
        }
      }

      if (isNull(index)) {
        throw new InternalError(
          `Internal error, could not index order-by reference: ${column}`,
        );
      }
      partitions.push(index);
    });

    return [newTargets, partitions];
  }

  compileOrderBy(
    targets: EvalTarget[],
    node?: OrderByExpression,
  ): [EvalTarget[], [number, "ASC" | "DESC", "FIRST" | "LAST"][]] {
    if (!node) {
      return [[], []];
    }
    const newTargets: EvalTarget[] = [];
    const targetExpressions = targets.map((it) => it.expression);
    const targetsNameMap: any = targets.reduce((acc: any, target, i) => {
      if (target.name) {
        acc[target.name] = i;
        acc[target.name.description] = i;
      }
      return acc;
    }, {});

    const orderSpec: [number, "ASC" | "DESC", "FIRST" | "LAST"][] = [];
    const size = Object.getOwnPropertySymbols(targetsNameMap).length;
    node.columns.forEach((spec) => {
      let index: number | null;
      const column = numberOrExpression(spec.column);
      if (typeof column === "number") {
        index = column - 1;
        if (index >= size || index < 0) {
          throw new CompilationError(
            `invalid ORDER-BY column index ${column}`,
            spec,
          );
        }
      } else {
        if (column instanceof ColumnExpression) {
          index = targetsNameMap[column.name];
        }
        if (isNull(index)) {
          const expr = this.compileExpression(column);
          index = targetExpressions.findIndex((it) => it.isEqual(expr));
          if (index === -1) {
            index = targets.length + newTargets.length;
            newTargets.push(
              new EvalTarget(expr, null, isAggregate(expr), false),
            );
          }
        }
      }

      if (isNull(index)) {
        throw new InternalError(
          `Internal error, could not index order-by reference: ${column}`,
        );
      }
      orderSpec.push([index, spec.direction, spec.nullHandling]);
    });
    return [newTargets, orderSpec];
  }

  compilePivotBy(
    targets: EvalTarget[],
    groupIndexes: number[],
    node?: PivotByExpression,
  ): number[] {
    if (!node) {
      return [];
    }
    const targetsNameMap: any = targets.reduce((acc: any, target, i) => {
      if (target.name) {
        acc[target.name] = i;
        acc[target.name.description] = i;
      }
      return acc;
    }, {});

    const indexes: number[] = [];
    const size = Object.getOwnPropertySymbols(targetsNameMap).length;
    node.columns.forEach((spec) => {
      let index: number | null;
      const column = spec;
      if (typeof column === "number") {
        index = column - 1;
        if (index >= size || index < 0) {
          throw new CompilationError(
            `invalid PIVOT-BY column index ${column}`,
            node,
          );
        }
      } else {
        if (column instanceof ColumnExpression) {
          index = targetsNameMap[column.name];
          if (isNull(index)) {
            throw new CompilationError(
              `PIVOT BY column ${column} is not in the targets list`,
              column,
            );
          }
        }
      }

      if (isNull(index)) {
        throw new CompilationError(
          `PIVOT BY column ${column} is not in the targets list`,
          column instanceof Expression ? column : undefined,
        );
      }
      indexes.push(index);
    });

    if (indexes[0] === indexes[1]) {
      throw new CompilationError(
        `the two PIVOT BY columns cannot be the same column'`,
        node,
      );
    }
    if (!groupIndexes.includes(indexes[1])) {
      throw new CompilationError(
        "the second PIVOT BY column must be a GROUP BY column",
        node,
      );
    }
    return indexes;
  }

  compileGroupBy(
    targets: EvalTarget[],
    node?: GroupByExpression,
  ): [EvalTarget[], number[], number] {
    const newTargets: EvalTarget[] = [];
    const allTargets: EvalTarget[] = [...targets];

    const targetExpressions = targets.map((it) => it.expression);
    const targetsNameMap: any = targets.reduce((acc: any, target, i) => {
      if (target.name) {
        acc[target.name] = i;
        acc[target.name.description] = i;
      }
      return acc;
    }, {});

    const groupIndexes: number[] = [];
    const size = Object.getOwnPropertySymbols(targetsNameMap).length;

    let havingIndex: number = -1;

    if (node) {
      node.columns.forEach((spec) => {
        let index: number | null;
        const column = numberOrExpression(spec);
        if (typeof column === "number") {
          index = column - 1;
          if (index >= size || index < 0) {
            throw new CompilationError(
              `invalid GROUP-BY column index ${column}`,
              node,
            );
          }
        } else {
          if (column instanceof ColumnExpression) {
            index = targetsNameMap[column.name];
          }
          if (isNull(index)) {
            const expr = this.compileExpression(column);
            if (isAggregate(expr)) {
              throw new CompilationError(
                `GROUP-BY expressions may not be aggregates: ${column}`,
                column,
              );
            }
            index = targetExpressions.findIndex((it) => it.isEqual(expr));
            if (index === -1) {
              index = allTargets.length;
              const value = new EvalTarget(expr, null, false);
              newTargets.push(value);
              allTargets.push(value);
            }
          }
        }

        if (isNull(index)) {
          throw new InternalError(
            `Internal error, could not index group-by reference: ${column}`,
            column instanceof Expression ? column : undefined,
          );
        }
        groupIndexes.push(index);
        if (isAggregate(allTargets[index].expression)) {
          throw new CompilationError(
            `GROUP-BY expressions may not reference aggregates: ${column}`,
            column instanceof Expression ? column : undefined,
          );
        }
      });
      if (node.having) {
        const expr = this.compileExpression(node.having);
        if (!isAggregate(expr)) {
          throw new CompilationError(
            `the GROUP-BY HAVING clause must be an aggregate expression: ${node.having}`,
            node.having,
          );
        }
        havingIndex = allTargets.length;
        const value = new EvalTarget(expr, null, true);
        newTargets.push(value);
        allTargets.push(value);
      }
    }

    const isGroupBy =
      targets.some((it) => it.aggregate && !it.windows.length) ||
      groupIndexes.length;

    if (this.options.supportImplicitGroupBy && isGroupBy) {
      targets.forEach((it, index) => {
        if (
          !it.aggregate &&
          !groupIndexes.includes(index) &&
          !it.windows.length
        ) {
          groupIndexes.push(index);
        }
      });
    }

    return [newTargets, groupIndexes, havingIndex];
  }

  compileAttribute(node: AttributeExpression): EvalNode | null {
    if (node.operand instanceof ColumnExpression) {
      if (!this.table.columns.has(node.operand.column)) {
        if (
          node.operand.column === this.table.name &&
          typeof node.name === "string"
        ) {
          return this.table.getColumn(node.name, false);
        }

        if (
          this.table.parent &&
          this.table.parent.name == node.operand.column
        ) {
          this.table.columns.set(
            node.operand.column,
            new EvalColumn(
              node.operand.column,
              this.table.parent.toStructure(),
            ),
          );
        }
      }
    }
    if (node.name instanceof CastExpression) {
      const fn = node.name.expr;
      if (fn instanceof FunctionExpression) {
        return this.compileExpression(
          new FunctionExpression(
            fn.parseInfo,
            fn.name,
            [node.operand, ...fn.args],
            fn.distinct,
            fn.filter,
            fn.window,
          ),
        );
      }
      throw new CompilationError(
        `Unsupported attribute expression: ${node.name}`,
        node.name,
      );
    } else if (node.name instanceof FunctionExpression) {
      const fn = node.name;
      return this.compileExpression(
        new FunctionExpression(
          fn.parseInfo,
          fn.name,
          [node.operand, ...fn.args],
          fn.distinct,
          fn.filter,
          fn.window,
        ),
      );
    }

    const operand = this.compileExpression(node.operand);
    const dType = structureFor(operand.type);
    if (dType) {
      const getter = dType.columns.get(node.name);
      if (isNull(getter)) {
        throw new CompilationError(
          `structured type has no attribute "${node.name}"`,
          node,
        );
      }
      return new EvalGetter(operand, getter, getter.type);
    }
    throw new CompilationError(
      `column is not structured: ${typeName(dType ?? operand.type)}`,
      node,
    );
  }

  compileSubscript(node: SubscriptExpression): EvalNode | null {
    const operand = this.compileExpression(node.operand);
    if (operand.type === Object) {
      return new EvalGetItem(operand, node.key);
    }
    throw new CompilationError(
      `column '${node.key}'::${typeName(operand.type)} type is not subscriptable:`,
      node,
    );
  }

  compileUpdateTable(node: UpdateTableExpression) {
    this.table = node.name;
    const columns: Record<string, EvalNode> = {};
    const where = this.compileExpression(node.where);
    const returning: Array<EvalNode> = this.expandTargets(node.returning).map(
      (it) => this.compileExpression(it),
    );

    for (const expr of node.values) {
      if (!(expr instanceof BooleanExpression)) {
        throw new CompilationError(`Syntax error at or near ${expr}`, expr);
      }
      if (expr.args.length !== 2 || expr.op !== "=") {
        throw new CompilationError(`Syntax error at or near ${expr}`, expr);
      }
      const column = expr.args[0];
      if (!(column instanceof ColumnExpression)) {
        throw new CompilationError(
          `Syntax error at or near ${expr}`,
          expr.args[0],
        );
      }
      if (!this.table.getColumn(column.name)) {
        throw new CompilationError(
          `column "${column.column}" of relation "${this.table.name}" does not exist`,
          column,
        );
      }
      columns[column.name] = this.compileExpression(expr.args[1]);
    }

    return new EvalUpdateTable(this.context, columns, returning, node, where);
  }

  compileCreateTable(node: CreateTableExpression) {
    const columns: Array<{
      name: symbol;
      type: DType;
      defaultValue?: EvalNode;
    }> = [];
    const query = node.query ? this.compileQuery(node.query) : undefined;
    if (this.context.tables.has(node.name)) {
      if (node.ifNotExists) {
        return new EvalConstant(1);
      }
      throw new CompilationError(
        `relation "${node.name}" already exists`,
        node,
      );
    }
    node.columns.forEach((col) => {
      const type = typeFor(col.type);
      const expr = col.defaultValue
        ? this.compileExpression(col.defaultValue)
        : undefined;
      if (type) {
        if (expr && !isSameType(type, expr.type)) {
          throw new CompilationError(
            `Invalid type ${typeName(expr.type)} for ${typeName(type)} column "${node.name}"."${col.name}"`,
            col.defaultValue,
          );
        }
        columns.push({
          name: Symbol(col.name),
          type: col.isArray ? [type] : type,
          defaultValue: expr,
        });
      } else {
        throw new CompilationError(`unrecognized type "${col.type}"`, node);
      }
    });
    if (!node.columns.length && query) {
      query.columns.forEach((col) => {
        if (col.name) {
          columns.push({ name: col.name, type: col.type });
        }
      });
    }
    return new EvalCreateTable(this.context, node, columns, query);
  }

  compileInsert(node: InsertExpression) {
    this.table = node.table;

    const returning: Array<EvalNode> = this.expandTargets(node.returning).map(
      (it) => this.compileExpression(it),
    );

    const columns = node.columns.length
      ? node.columns
      : [...this.table.columns.keys()];
    const row = node.values.find((it) => it.length != columns.length);
    if (row) {
      throw new CompilationError(
        `column names and values mismatch: expected ${columns.length} value(s) but got ${row.length}\n${row.toString()}`,
        node,
      );
    }

    const rows: Record<string, EvalNode>[] = [];
    const initialColumns = [...this.table.columns.keys()].reduce(
      (acc: Record<string, EvalNode>, key) => {
        const column = this.table.getColumn(key);
        acc[key] = column;
        return acc;
      },
      {},
    );

    node.values.forEach((rowValues) => {
      const row = { ...initialColumns };
      columns.forEach((column, i) => {
        if (isNull(row[column])) {
          throw new CompilationError(
            `column "${column}" not found in table "${node.table}"`,
            rowValues[i],
          );
        }
        const expr = this.compileExpression(rowValues[i]);
        row[column] = expr;
      });
      rows.push(row);
    });
    return new EvalInsert(node, this.table, rows, returning);
  }
}

export function getColumnsAndAggregates(
  node: EvalNode,
  columns: EvalColumn[] = [],
  aggregates: EvalAggregator[] = [],
  excludeWindowFunction: boolean = false,
): [EvalColumn[], EvalAggregator[]] {
  if (excludeWindowFunction && node instanceof EvalWindow) {
    return [columns, aggregates];
  }
  if (node instanceof EvalAggregator) {
    aggregates.push(node);
  } else if (node instanceof EvalColumn) {
    columns.push(node);
  } else if (
    node instanceof EvalConstantSubquery1D ||
    node instanceof EvalConstantSubqueryValue ||
    node instanceof EvalQuery
  ) {
    return [columns, aggregates];
  } else if (node) {
    node.childNodes.forEach((it) => {
      getColumnsAndAggregates(it, columns, aggregates, excludeWindowFunction);
    });
  }

  return [columns, aggregates];
}

export function isAggregate(node: EvalNode): boolean {
  const [_, aggregates] = getColumnsAndAggregates(node);
  return aggregates.length > 0;
}

function numberOrExpression(expr: Expression): Expression | number {
  if (expr instanceof LiteralExpression && typeof expr.value === "number") {
    return expr.value;
  }
  return expr;
}
