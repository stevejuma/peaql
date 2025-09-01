import { InternalError } from "./errors";
import { Context, MutableTableProperties, Table, TableModel } from "./query";

export * from "./parser";
export * from "./query";
export * from "./decimal";
export * from "./errors";
export * from "./models";

export type CreateDatabaseProperties = {
  data: TableModel;
  options?: Partial<MutableTableProperties>;
};

export function createDatabase(
  data: Record<string, CreateDatabaseProperties>,
): Context {
  const tables: Array<Table> = [];
  for (const [name, record] of Object.entries(data)) {
    const table = Table.fromJSON(record.data, record.options);
    if (table.name !== name) {
      throw new InternalError(
        `Table name: ${table.name} doesn't match model name: ${name}`,
      );
    }
    tables.push(table);
  }
  return Context.create(...tables);
}
