/* eslint-disable @typescript-eslint/no-explicit-any */
import { DateTime, Duration } from "luxon";
import { getColumnsAndAggregates } from "./compiler";
import {
  Allocator,
  EvalAggregator,
  EvalConstant,
  EvalQuery,
  findFunction,
} from "./nodes";
import {
  DType,
  EvalNode,
  getValueByDotNotation,
  isEqual,
  isNull,
  typeFor,
} from "./types";
import { windowFunction } from "./window";

function toArray(data: any, columns: any) {
  data ||= {};
  const keys = Object.getOwnPropertySymbols(columns);
  const row = new Array(keys.length);
  keys.forEach((k) => {
    row[columns[k]] = data[k];
  });
  return row;
}

export function executeSelect(
  query: EvalQuery,
  globalContext?: any,
): [{ name: symbol; type: DType }[], unknown[][]] {
  // Figure out the result types that describe what we return.
  // Indexes of the columns for result rows and order rows.

  const targets = query.targets.map((it, index) => {
    const aggregates = it.windows.map((it) => it.aggregates).flat();
    if (aggregates.length) {
      const [_, aggrs] = getColumnsAndAggregates(it.expression, [], [], true);
      aggregates.push(...aggrs);
    }
    return {
      column: it,
      name: it.name,
      expression: it.expression,
      stores: [] as any[][],
      windows: it.windows,
      aggregates,
      type: it.expression.type,
      index,
      visible: it.visible && it.name,
    };
  });

  const columns = targets.filter((it) => it.visible);

  if (!columns.length) {
    return [[], []];
  }
  // Pre-compute lists of the expressions to evaluate.
  const groupIndexes = query.groupIndexes
    ? new Set<number>(query.groupIndexes)
    : null;
  const orderSpec = query.orderSpec;

  // Dispatch between the non-aggregated queries and aggregated queries.
  const where = query.where;
  let rows: unknown[][] = [];

  const targetExprs = query.targets.map((it) => it.expression);
  const isWindow = targets.some((it) => it.windows.length);

  //  Precompute lists of non-aggregate and aggregate expressions to
  //  evaluate. For aggregate targets, we hunt down the aggregate
  //  sub-expressions to evaluate, to avoid recursion during iteration.
  const aggr: Array<EvalAggregator> = [];

  const nonAggr: EvalNode[] = [];
  targets.forEach((target) => {
    if (groupIndexes.has(target.index)) {
      nonAggr.push(target.expression);
    } else if (target.aggregates.length) {
      aggr.push(...target.aggregates);
    } else {
      const [_, aggrExpr] = getColumnsAndAggregates(target.expression);
      aggr.push(...aggrExpr);
    }
  });

  const isGroup =
    groupIndexes.size > 0 || targets.some((it) => it.column.hasAggregate);

  if (isGroup) {
    //  This is an aggregated query.
    // # Note: it is possible that there are no aggregates to compute here. You could
    // # have all columns be non-aggregates and group-by the entire list of columns.

    // # Pre-allocate handles in aggregation nodes.
    const allocator = new Allocator();
    aggr.forEach((it) => it.allocate(allocator));
    const aggregates = new Map<unknown[], any[]>();

    for (const ctx of query.table) {
      const context = globalContext
        ? { ...globalContext, ...(ctx as any) }
        : ctx;
      if (isNull(where) || where.resolve(context)) {
        const id = nonAggr.map((it) => it.resolve(context));
        let key = [...aggregates.keys()].find((k) => {
          return isEqual(k, id);
        });
        if (!key) {
          key = id;
          const store = allocator.createStore();
          aggr.forEach((it) => it.initialize(store));
          aggregates.set(key, store);
        }
        const store = aggregates.get(key);
        aggr
          .filter((it) => it.accept(context))
          .forEach((it) => it.update(store, context));
      }
    }

    for (const [key, store] of aggregates.entries()) {
      const values: unknown[] = [];
      aggr.forEach((it) => it.finalize(store));
      let index = 0;
      targets.forEach((target) => {
        if (groupIndexes.has(target.index)) {
          values.push(key[index]);
          index++;
        } else {
          if (target.aggregates.length) {
            values.push(target.aggregates.map((it) => it.resolve()));
          } else {
            values.push(target.expression.resolve());
          }
        }
      });

      if (query.havingIndex !== -1) {
        if (!values[query.havingIndex]) {
          continue;
        }
      }
      rows.push(values);
    }

    if (aggregates.size == 0 && query.columns.every((it) => it.aggregate)) {
      const store = allocator.createStore();
      aggr.forEach((it) => it.initialize(store));
      aggr.forEach((it) => it.finalize(store));
      rows.push(targets.map((it) => it.expression.resolve()));
    }
  } else {
    // This is a non-aggregated query.
    // Iterate over all the postings once.

    let isEmpty: boolean = true;
    for (const ctx of query.table) {
      const context = globalContext
        ? { ...globalContext, ...(ctx as any) }
        : ctx;
      isEmpty = false;
      if (isNull(where) || where.resolve(context)) {
        const data = targets.map((it) => {
          if (it.windows.length) {
            return context;
          } else {
            return it.expression.resolve(context);
          }
        });
        rows.push(data);
      }
    }

    if (
      isEmpty &&
      query.targets.every((it) => it.expression instanceof EvalConstant)
    ) {
      rows.push(targetExprs.map((it) => it.resolve(null)));
    }
  }

  if (isWindow) {
    const windowTargets = targets.filter((it) => it.windows.length);

    const windowData: any[] = [];
    rows.forEach((data) => {
      const record: any = {};
      targets.forEach((target) => {
        if (target.name) {
          record[target.name] = data[target.index];
        }
      });
      windowData.push(record);
    });

    const columnsMap = query.targets.reduce((obj: any, value, i) => {
      if (value.name) {
        obj[value.name] = i;
      }
      return obj;
    }, {});

    windowTargets.forEach((target) => {
      target.windows.forEach((windowExpr) => {
        const window = windowExpr.window;
        const props: Partial<{
          partitionBy: (item: any) => any;
          orderBy: (a: any, b: any) => number;
          orderValues: (a: any) => [number, "ASC" | "DESC"][];
        }> = {};

        if (window.orderBy) {
          props.orderBy = (a, b) => {
            return sortColumns(
              window.orderBy,
              toArray(a, columnsMap),
              toArray(b, columnsMap),
            );
          };

          props.orderValues = (a) => {
            const values = toArray(a, columnsMap);
            return (window.orderBy || []).map(([index, direction]) => [
              orderValue(values[index]),
              direction,
            ]);
          };
        }

        if (window.partitionBy) {
          props.partitionBy = (item) => {
            const row = toArray(item, columnsMap);
            const value = window.partitionBy.map((it) => row[it]);
            return value;
          };
        }

        target.aggregates.forEach((aggr, i) => {
          aggr.resolver = (context: any) => {
            return (getValueByDotNotation(context, target.name) as any[])[i];
          };
        });

        const data = windowFunction(
          windowData,
          (data, i, fullPartition) => {
            const windowState = {
              data,
              index: i,
              fullPartition,
              orderValue: (item: any) => {
                const values = toArray(item, columnsMap);
                return (window.orderBy || []).map(([index, direction]) => [
                  orderValue(values[index]),
                  direction,
                ]);
              },
            };
            data = isGroup
              ? data
              : data.map((it) => getValueByDotNotation(it, target.name));
            return windowExpr.execute(data, windowState);
          },
          window.frame,
          props,
        );

        const id = Symbol("window:" + target.name.description);
        data.forEach((value, i) => (windowData[i][id] = value));

        windowExpr.expression.resolver = (context: any) => {
          return getValueByDotNotation(context, id);
        };

        data.forEach((_, i) => {
          rows[i][target.index] = target.expression.resolve(windowData[i]);
        });
      });
    });
  }
  rows = multiColumnSort(rows, orderSpec);
  rows = rows.map((row) => {
    return columns.map((it) => row[it.index]);
  });

  if (query.distinct) {
    const existing = [...rows];
    rows = [];
    existing.forEach((entry) => {
      if (rows.some((it) => isEqual(it, entry))) {
        return;
      }
      rows.push(entry);
    });
  }

  if (query.limit) {
    rows = rows.slice(0, query.limit);
  }

  return [columns.map((c) => ({ name: c.name, type: c.type })), rows];
}

function multiColumnSort(
  data: unknown[][],
  orderSpec: [number, "ASC" | "DESC"][],
  nullHandling: "first" | "last" | "default" = "default",
): unknown[][] {
  if (!orderSpec.length) {
    return data.slice();
  }
  return data
    .slice()
    .sort((a, b) => sortColumns(orderSpec, a, b, nullHandling));
}

function orderValue(value: any): number {
  if (isNull(value)) {
    return null;
  } else if (typeof value === "number") {
    return value;
  } else if (typeof value === "string") {
    let hash = 0;
    for (let i = 0; i < value.length; i++) {
      hash = (hash << 5) - hash + value.charCodeAt(i);
    }
    return hash >>> 0;
  } else if (value instanceof DateTime) {
    return value.toMillis();
  } else if (value instanceof Duration) {
    return value.toMillis();
  } else if (value instanceof Date) {
    return value.getTime();
  }
  return findFunction("number", [typeFor(value)])?.operator(value) ?? value;
}

function sortColumns(
  orderSpec: [number, "ASC" | "DESC"][],
  a: unknown[],
  b: unknown[],
  nullHandling: "first" | "last" | "default" = "default",
) {
  for (let i = 0; i < orderSpec.length; i++) {
    const colIndex = orderSpec[i][0];
    const direction = orderSpec[i][1];

    const aValue = a[colIndex];
    const bValue = b[colIndex];

    if (nullHandling !== "default") {
      if (isNull(aValue) && isNull(bValue)) {
        continue;
      } else if (isNull(aValue)) {
        return nullHandling === "first" ? -1 : 1;
      } else if (isNull(bValue)) {
        return nullHandling === "first" ? 1 : -1;
      }
    }

    if (aValue < bValue) {
      return direction === "ASC" ? -1 : 1;
    } else if (aValue > bValue) {
      return direction === "ASC" ? 1 : -1;
    }
  }
  return 0;
}