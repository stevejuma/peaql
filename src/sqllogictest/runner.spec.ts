import { describe, it, ExpectStatic, expect } from "vitest";
import { cleanResults, parseCommands } from "./runner";
import { Context } from "../lib/query/context";
import md5 from "md5";

// const testfiles = import.meta.glob("./specs/**/*.spec", { query: "?raw" });
const testfiles = import.meta.glob("./specs/**/*.test", { query: "?raw" });

const mimic = "sqlite";

for (const path in testfiles) {
  describe.concurrent(path, async () => {
    const module = (await testfiles[path]()) as { default: string };
    const context = Context.create();
    const commands = parseCommands(module.default);
    const specs: Record<string, (expect: ExpectStatic) => void> = {};
    const setup: Record<string, () => void> = {};

    commands.forEach((fragment) => {
      if (fragment.command === "setThreshold") {
        return;
      }

      if (
        fragment.skipif &&
        fragment.skipif.length &&
        -1 < fragment.skipif.indexOf(mimic)
      ) {
        return;
      }

      if (
        fragment.onlyif &&
        fragment.onlyif.length &&
        fragment.onlyif.indexOf(mimic) < 0
      ) {
        return;
      }
      const type = fragment.result.type;
      if (["void", "statement"].includes(type)) {
        setup[fragment.sql] = () => {
          if (fragment.expectSuccess) {
            context.execute(fragment.sql);
          } else {
            expect(() => context.execute(fragment.sql)).toThrow();
          }
        };
      } else {
        specs[fragment.sql] = (expect) => {
          if (!fragment.expectSuccess) {
            expect(() => context.execute(fragment.sql)).toThrow();
            return;
          }
          const response = context.execute(fragment.sql);
          if (type === "list") {
            const [_, data] = response;
            const result = cleanResults([...data], fragment.result.sort);
            expect(result).toEqual(fragment.result.values);
          } else if (type === "hash") {
            const [_, data] = response;
            const result = cleanResults([...data], fragment.result.sort);
            expect(result.length).toEqual(+fragment.result.amount);
            expect(md5(result.join("\n") + "\n")).toEqual(fragment.result.hash);
          }
        };
      }
    });

    for (const [key, value] of Object.entries(setup)) {
      it(key, value);
    }

    for (const [query, value] of Object.entries(specs)) {
      it.concurrent(query, async ({ expect }) => {
        value(expect);
      });
    }
  });
}
