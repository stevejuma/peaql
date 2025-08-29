import { Context, Table } from "./query";

export * from "./parser";
export * from "./query";
export * from "./decimal";
export * from "./errors";
export * from "./models";

export type CreateDatabaseProperties = {
  columns: Record<string, string>;
  data: Array<Record<string, unknown>>;
};

export function createDatabase(
  data: Record<string, CreateDatabaseProperties>,
): Context {
  const tables: Array<Table> = [];
  for (const [name, records] of Object.entries(data)) {
    tables.push(Table.fromObject(name, records.data));
  }
  return new Context().withTables(...tables);
}
