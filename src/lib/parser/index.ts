export * from "./ast";
export * from "./parser";
export { parser } from "./bql.grammar";

import { LRParser } from "@lezer/lr";
import { parser } from "./bql.grammar";
import { LRLanguage, LanguageSupport } from "@codemirror/language";

export const beancountQueryLanguage = LRLanguage.define({
  name: "bql",
  parser: parser,
});

export function bql(parser?: LRParser) {
  if (parser) {
    return new LanguageSupport(
      LRLanguage.define({
        name: "bql",
        parser: parser,
      }),
    );
  }
  return new LanguageSupport(beancountQueryLanguage);
}
