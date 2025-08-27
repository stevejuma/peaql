import { Context, Table } from "./query";

export * from "./parser";
export * from "./query";
export * from "./decimal";
export * from "./errors";
export * from "./models";

export function createDatabase(data: Record<string, Array<Record<string, unknown>>>): Context {
    const tables: Array<Table> = [];
    for (const [name, records] of Object.entries(data)) {
        tables.push(Table.fromObject(name, records));
    }
    return new Context().withTables(...tables);
}