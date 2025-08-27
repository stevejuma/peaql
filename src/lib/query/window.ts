import { isEqual } from "./types";

/* eslint-disable @typescript-eslint/no-explicit-any */
type FrameType = "ROWS" | "GROUPS" | "RANGE";

type FrameClause = {
  type: FrameType | string;
  preceding?: number; // Number of rows/groups/ranges before
  following?: number; // Number of rows/groups/ranges after
  exclude: string;
};

type WindowFunction<T, R> = (
  window: T[],
  currentIndex: number,
  fullData: T[],
) => R;

const ALL = Symbol("ALL");

export function windowFunction<T, R>(
  data: T[],
  operation: WindowFunction<T, R>,
  frameClause: FrameClause = {
    type: "ROWS",
    preceding: 0,
    following: 0,
    exclude: "NONE",
  }, // Window frame clause
  props: Partial<{
    partitionBy: (item: T) => any;
    orderBy: (a: T, b: T) => number;
    orderValues: (a: T) => [number, "ASC" | "DESC"][];
  }> = {},
): R[] {
  data = [...data];
  // Step 1: Partition data
  const partitions = new Map<any, T[]>();
  const partitionBy = props.partitionBy ?? ((_: T) => ALL);
  const orderBy = props.orderBy;
  const records = new Map<any, R>();

  for (const item of data) {
    const id = partitionBy(item);
    let key = [...partitions.keys()].find((it) => isEqual(it, id));
    if (!key) {
      key = id;
      partitions.set(key, []);
    }
    partitions.get(key)!.push(item);
  }

  // Step 2: Process each partition separately
  for (const [_, partition] of partitions) {
    // Step 2.1: Sort partition if ordering is provided
    if (orderBy) {
      partition.sort(orderBy);
    }

    // Step 2.2: Compute window function
    for (let i = 0; i < partition.length; i++) {
      let window: T[];

      if (frameClause.type === "ROWS") {
        // ROWS BETWEEN X PRECEDING AND Y FOLLOWING
        const rowStart = Math.max(0, i - (frameClause.preceding ?? 0));
        const rowEnd = Math.min(
          partition.length,
          i + (frameClause.following ?? 0) + 1,
        );
        window = partition.slice(rowStart, rowEnd);
      } else if (frameClause.type === "GROUPS") {
        // GROUPS BETWEEN X PRECEDING AND Y FOLLOWING
        const groupValues = new Map<any, T[]>();
        for (const item of partition) {
          const id = props.orderValues(item);
          let key = [...groupValues.keys()].find((it) => isEqual(it, id));
          if (!key) {
            key = id;
            groupValues.set(key, []);
          }
          groupValues.get(key)!.push(item);
        }

        const orderedGroups = Array.from(groupValues.values());
        const groupIndex = orderedGroups.findIndex((group) =>
          group.includes(partition[i]),
        );
        const groupStart = Math.max(
          0,
          groupIndex - (frameClause.preceding ?? 0),
        );
        const groupEnd = Math.min(
          orderedGroups.length,
          groupIndex + (frameClause.following ?? 0) + 1,
        );
        window = orderedGroups.slice(groupStart, groupEnd).flat();
      } else if (frameClause.type === "RANGE") {
        // RANGE BETWEEN X PRECEDING AND Y FOLLOWING (assuming numeric ordering)
        if (!props.orderValues) {
          throw new Error("RANGE requires an ORDER BY clause.");
        }

        const [refValue, dir] = props.orderValues(partition[i])[0];
        window = partition.filter((item) => {
          const [value] = props.orderValues(item)[0] || [];
          if (dir === "ASC") {
            return (
              refValue - (frameClause.preceding ?? 0) <= value &&
              value <= refValue + (frameClause.following ?? 0)
            );
          }
          return (
            refValue + (frameClause.preceding ?? 0) >= value &&
            value >= refValue - (frameClause.following ?? 0)
          );
        });
      } else {
        throw new Error(`Unsupported frame type: ${frameClause.type}`);
      }

      if (frameClause.exclude === "CURRENT") {
        window = window.filter((it) => it !== partition[i]);
      } else if (frameClause.exclude === "GROUP") {
        if (props.orderValues) {
          const value = props.orderValues(partition[i]);
          window = window.filter(
            (it) => !isEqual(props.orderValues(it), value),
          );
        } else {
          window = [];
        }
      } else if (frameClause.exclude === "TIES") {
        if (props.orderValues) {
          const value = props.orderValues(partition[i]);
          window = window.filter(
            (it) =>
              it === partition[i] || !isEqual(props.orderValues(it), value),
          );
        } else {
          window = [];
        }
      }
      records.set(partition[i], operation(window, i, partition));
    }
  }
  return data.map((it) => records.get(it));
}
