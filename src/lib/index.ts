import { Context, MutableTableProperties, Table } from "./query";

export * from "./parser";
export * from "./query";
export * from "./decimal";
export * from "./errors";
export * from "./models";

export type CreateDatabaseProperties = {
  data: Array<Record<string, unknown>>;
  options?: Partial<MutableTableProperties>;
};

export function createDatabase(
  data: Record<string, CreateDatabaseProperties>,
): Context {
  const tables: Array<Table> = [];
  for (const [name, record] of Object.entries(data)) {
    const table = Table.fromObject(name, record.data, record.options);
    tables.push(table);
  }
  return new Context().withTables(...tables);
}
