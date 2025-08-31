/* eslint-disable @typescript-eslint/no-unsafe-function-type */
/* eslint-disable @typescript-eslint/no-explicit-any */

import { DateTime, Duration } from "luxon";
import { Decimal, isValidNumber, parseNumber } from "../decimal";

export const NULL = class {
  toString() {
    return "NULL";
  }
};

export const INTEGER = class {
  toString() {
    return "integer";
  }
};

export const LIST = class {
  toString() {
    return "List<..>";
  }
};

export const ASTERISK = class {
  toString() {
    return "*";
  }
};

export const VARARG = class {
  toString() {
    return "...";
  }
};

export type DType = Function | [DType] | Array<DType>;

export class TypeDef {
  readonly extensions = new Set<DType>();
  constructor(
    readonly type: Function,
    readonly name: string,
    readonly props: Partial<{
      isType: (obj: any) => boolean;
      cast: (obj: any, context: TypeDef) => any;
      extensions: Function[];
    }> = {},
  ) {
    (props.extensions || []).forEach((it) => this.extensions.add(it));
  }

  public cast(obj: any) {
    if (this.isInstanceOf(obj)) {
      return obj;
    }
    if (this.props.cast) {
      return this.props.cast(obj, this);
    }
  }

  public toString() {
    return this.name;
  }

  isInstanceOf(obj: any): boolean {
    return (
      obj === this.type ||
      obj?.constructor === this.type ||
      obj instanceof this.type ||
      (this.props.isType && this.props.isType(obj)) ||
      false
    );
  }
}

export function normalizeColumns(
  columns: Array<{ name: symbol; type: DType }>,
) {
  return columns.map((it) => {
    return { name: it.name.description, type: it.type };
  });
}

export abstract class Structure {
  public static name: string;
  public static type: "default" | "join" = "default";
  public static columns: Map<string, EvalNode>;
  public static functions: Record<string, Array<Operation>> = {};
  public static wildcardColumns: string[] = [];

  static findFunction(fn: string, operands: any[]) {
    const matches = (this.functions[fn.toLowerCase()] || [])
      .filter((it) => it.matches(operands))
      .sort((a, b) => a.sortKey - b.sortKey);
    return matches[0];
  }
}

const TYPES = new Map<DType | string, TypeDef>();
const ALIASES = new Map<DType, DType>();
const STRUCTURED = new Map<DType, typeof Structure>();

export function registerType(
  type: Function,
  name: string,
  props: Partial<{
    isType: (obj: any) => boolean;
    aliases: string[];
    cast: (obj: any, context: TypeDef) => any;
    extensions: Function[];
  }> = {},
) {
  const typeDef = new TypeDef(type, name, props);
  TYPES.set(typeDef.type, typeDef);
  TYPES.set(typeDef.name.toLowerCase(), typeDef);
  if (props.aliases) {
    props.aliases.forEach((it) => TYPES.set(it, typeDef));
  }
}

export function typeFor(name: string): DType {
  name = name.toLowerCase();
  if (TYPES.has(name)) {
    return TYPES.get(name).type;
  }
  return null;
}

export function registerAlias(type: Function, alias: Function) {
  ALIASES.set(type, alias);
}

export function registerStructure(type: Function, structure: typeof Structure) {
  STRUCTURED.set(type, structure);
}

export function structureFor(type?: DType | null) {
  if (!type) {
    return null;
  }
  if ((type as any).prototype instanceof Structure) {
    return type as typeof Structure;
  }
  return STRUCTURED.get(type);
}

registerType(NULL, "null", { isType: isNull });
registerType(ASTERISK, "*");
registerType(INTEGER, "integer", {
  isType: (obj) => typeof obj === "number" && Number.isInteger(obj),
  extensions: [Number],
  aliases: ["int", "integer", "real"],
});
registerType(Number, "number", {
  isType: (obj) => typeof obj === "number",
  aliases: ["number", "double"],
});
registerType(Boolean, "boolean", {
  isType: (obj) => typeof obj === "boolean",
  aliases: ["bool"],
});
registerType(String, "string", {
  isType: (obj) => typeof obj === "string",
  aliases: ["str", "text", "varchar"],
});

export const DATE_TIME_FORMATS = [
  // Date + time
  "yyyy-MM-dd'T'HH:mm:ss.SSSZZ", // 2025-08-30T14:23:45.123+02:00
  "yyyy-MM-dd'T'HH:mm:ssZZ", // 2025-08-30T14:23:45+02:00
  "yyyy-MM-dd'T'HH:mm:ss", // 2025-08-30T14:23:45
  "yyyy-MM-dd HH:mm:ss.SSSZZ", // 2025-08-30 14:23:45.123+02:00
  "yyyy-MM-dd HH:mm:ssZZ", // 2025-08-30 14:23:45+02:00
  "yyyy-MM-dd HH:mm:ss.SSS", // 2025-08-30 14:23:45.123
  "yyyy-MM-dd HH:mm:ss", // 2025-08-30 14:23:45

  // US-style
  "M/d/yyyy HH:mm:ss", // 8/3/2025 14:23:45 (single-digit month/day)
  "MM/dd/yyyy HH:mm:ss", // 08/30/2025 14:23:45
  "M/d/yyyy h:mm:ss a", // 8/3/2025 2:23:45 PM
  "MM/dd/yyyy h:mm:ss a", // 08/30/2025 2:23:45 PM

  // European-style
  "d.M.yyyy HH:mm:ss", // 3.8.2025 14:23:45 (single-digit day/month)
  "dd.MM.yyyy HH:mm:ss", // 30.08.2025 14:23:45

  // Compact ISO
  "yyyyMMdd'T'HHmmss", // 20250830T142345

  // Time only
  "HH:mm:ss", // 14:23:45

  // With named time zone
  "yyyy-MM-dd HH:mm:ss z", // 2025-08-30 14:23:45 Europe/Berlin

  // RFC 2822
  "EEE, d LLL yyyy HH:mm:ss ZZZ", // Sat, 3 Aug 2025 14:23:45 +0200 (single-digit day)
  "EEE, dd LLL yyyy HH:mm:ss ZZZ", // Sat, 30 Aug 2025 14:23:45 +0200,

  // Date only (all variants)
  "yyyy-MM-dd", // 2025-08-30 (ISO)
  "yyyy-M-d",
  "yyyy-M-dd",
  "yyyy-MM-d",
  "yyyy-MM-dd",
  "M/d/yyyy", // 8/3/2025 (single-digit month/day)
  "MM/dd/yyyy", // 08/30/2025 (US)
  "d.M.yyyy", // 3.8.2025 (single-digit day/month)
  "dd.MM.yyyy", // 30.08.2025 (European)
  "yyyyMMdd", // 20250830 (Compact ISO)
  "EEE, d LLL yyyy", // Sat, 3 Aug 2025 (single-digit day)
  "EEE, dd LLL yyyy", // Sat, 30 Aug 2025 (RFC 2822)
];

export function parseDateTime(value: string) {
  const parsers: Array<(value: string) => DateTime> = [
    DateTime.fromISO,
    DateTime.fromSQL,
    DateTime.fromHTTP,
    DateTime.fromRFC2822,
  ];

  for (const parse of parsers) {
    const dt = parse(value);
    if (dt && dt.isValid) {
      return dt;
    }
  }

  for (const format of DATE_TIME_FORMATS) {
    const patterns = [
      ...new Set([
        format,
        format.replaceAll("-", "/"),
        format.replaceAll("-", "."),
        format.replaceAll("/", "-"),
        format.replaceAll("/", "."),
        format.replaceAll(".", "/"),
        format.replaceAll(".", "-"),
      ]),
    ];

    for (const pattern of patterns) {
      const dt = DateTime.fromFormat(value, pattern);
      if (dt && dt.isValid) {
        return dt;
      }
    }
  }
}

registerType(DateTime, "datetime", {
  aliases: ["date", "timestamp", "timestampz"],
  cast: (obj) => {
    if (typeof obj === "string") {
      return parseDateTime(obj);
    }
  },
});
registerType(Decimal, "numeric", {
  aliases: ["decimal"],
  cast: (obj) => {
    if (typeof obj === "number") return new Decimal(obj);
    if (obj instanceof Decimal) return obj;
    if (typeof obj === "string" && isValidNumber(obj)) {
      return parseNumber(obj);
    }
  },
});
registerType(Duration, "duration");
registerType(VARARG, "vararg");
registerType(LIST, "list", {
  isType: (obj) => obj instanceof Set || Array.isArray(obj),
  aliases: ["set", "array"],
});
registerType(Object, "object", { aliases: ["any"] });

function _typeOf(value: unknown): DType {
  if (isNull(value)) {
    return NULL;
  } else if (value instanceof EvalNode) {
    return value.type;
  } else if (Array.isArray(value)) {
    const types = [...new Set(value.map((it) => typeOf(it)))];
    return types.length == 1 ? [types[0]] : [Object];
  } else if (typeof value === "function") {
    const typeDef = TYPES.get(value);
    if (typeDef) {
      return typeDef.type;
    }
    return value;
  } else if (typeof value === "object" && value?.constructor) {
    return typeOf(value.constructor);
  }

  return (
    [...TYPES.values()].find((it) => it.isInstanceOf(value))?.type ?? Object
  );
}

export function typeCast(value: unknown, type?: DType) {
  type ||= typeOf(value);
  const typeDef = [...TYPES.values()].find((it) => it.type === type);
  return typeDef?.cast(value);
}

export function typeOf(value: unknown): DType {
  const type = _typeOf(value);
  if (ALIASES.has(type)) {
    return ALIASES.get(type);
  }
  return type;
}

export function typeName(value: any): string {
  if (value === Number) {
    return "number";
  } else if (value === String) {
    return "string";
  } else if (value === Boolean) {
    return "boolean";
  } else if (value === DateTime) {
    return "datetime";
  } else if (value === Object) {
    return "object";
  } else if (Array.isArray(value)) {
    const types = [...new Set(value.map((it) => typeName(it)))];
    return types.length === 1 ? `Array<${types[0]}>` : "Array<Object>";
  }

  const type = [...TYPES.values()].find(
    (it) => it.type !== Object && it.isInstanceOf(value),
  );
  if (type) {
    return type.name;
  } else if (typeof value === "function" && value.name) {
    return value.name.toLowerCase();
  }
  return typeName(typeOf(value));
}

export function isEqual(a: any, b: any): boolean {
  if (isNull(a)) {
    return isNull(b);
  } else if (a instanceof EvalNode) {
    return a.isEqual(b);
  } else if (a instanceof DateTime) {
    return b instanceof DateTime && a.toMillis() === b.toMillis();
  } else if (a instanceof Duration) {
    return b instanceof Duration && a.toMillis() === b.toMillis();
  } else if (Array.isArray(a)) {
    return (
      Array.isArray(b) &&
      a.length === b.length &&
      a.every((v, i) => isEqual(v, b[i]))
    );
  } else if (a && typeof a.isEqual === "function") {
    return a.isEqual(b);
  } else if (b && typeof b.isEqual === "function") {
    return b.isEqual(a);
  }
  return a === b;
}

export function isNull(value: unknown) {
  return value === null || value === undefined || Number.isNaN(value);
}

export function getValueByDotNotation(
  obj: unknown,
  path: string | symbol,
  aliases: Record<string, string[]> = {},
): unknown {
  if (obj && typeof path === "symbol") {
    return (obj as any)[path];
  }

  if (isNull(obj) || !(typeof path === "string")) {
    return null;
  }
  const keys = path.replace(/\[(\d+)\]/g, ".$1").split("."); // Converts array indices to dot notation
  let result: unknown = obj;

  for (let key of keys) {
    if (result === undefined || result === null) {
      return undefined; // Return undefined if any key doesn't exist or if result becomes null
    }
    const names: string[] = [...(aliases[key] ?? []), key];
    key =
      names.find(
        (k) => typeof result === "object" && result !== null && k in result,
      ) ?? key;

    // Check if the property is a function
    if (
      typeof result === "object" &&
      typeof (result as Record<string, unknown>)[key] === "function"
    ) {
      result = (result as Record<string, () => unknown>)[key](); // Call the function
    } else {
      const value = (result as any)[key]; // Otherwise, just access the property
      if (value === undefined) {
        return Object.getOwnPropertySymbols(result)
          .filter((it) => it.description === key)
          .map((it) => (result as any)[it])
          .find((it) => it !== undefined);
      } else {
        result = value;
      }
    }
  }
  return result;
}

export abstract class EvalNode {
  static inTypes: DType[] = [];
  constructor(
    public type: DType,
    public context: any = {},
  ) {}

  abstract resolve(context?: any): any;
  abstract isEqual(obj: any): boolean;

  abstract get childNodes(): EvalNode[];

  traverse(
    predicate: (item: EvalNode) => boolean = () => true,
    traverse: (item: EvalNode) => boolean = () => true,
  ): EvalNode[] {
    const children: EvalNode[] = [];
    this.childNodes.forEach((it) => {
      if (predicate(it)) {
        children.push(it);
      }
      if (traverse(it)) {
        children.push(...it.traverse(predicate));
      }
    });
    return [...new Set(children)];
  }
}

export class Operation {
  constructor(
    readonly input: DType[],
    readonly output: DType,
    readonly operator: (...args: any[]) => any,
    readonly creator: (...args: any[]) => EvalNode,
  ) {}

  create(...args: any[]): EvalNode {
    return this.creator(...args);
  }

  matches(args: any[]) {
    const signature = args.map((it) => typeOf(it));
    let inputs: DType[] = this.input;
    const index = this.input.findIndex((it) => it === VARARG);
    if (index !== -1) {
      inputs = [
        ...inputs.slice(0, index),
        ...Array.from({ length: args.length - index }).fill(
          this.input[index + 1] ?? Object,
        ),
      ] as DType[];
    }
    if (signature.length <= inputs.length) {
      for (let i = 0; i < inputs.length; i++) {
        if (this.matchesType(inputs[i], signature[i])) {
          continue;
        } else if (Array.isArray(inputs[i]) && Array.isArray(signature[i])) {
          const a = inputs[i] as Array<unknown>;
          const b = signature[i] as Array<unknown>;
          if (a.every((v, i) => this.matchesType(v as DType, b[i] as DType))) {
            continue;
          }
        }
        return false;
      }
      return true;
    }
    return false;
  }

  matchesType(input: DType, signature: DType) {
    const type = TYPES.get(signature);
    return (
      input === Object ||
      input === signature ||
      (type && type.extensions.has(input))
    );
  }

  get sortKey() {
    return this.input.filter((it) => it === Object).length;
  }
}

export function isSameType(a: DType, b: DType): boolean {
  if (a === b) {
    return true;
  } else if (Array.isArray(a) && Array.isArray(b)) {
    return (
      a.length === b.length &&
      a.every((v, i) => isSameType(v as DType, b[i] as DType))
    );
  }
  const aType = TYPES.get(a);
  const bType = TYPES.get(b);
  return (
    (aType && aType.extensions.has(b)) || (bType && bType.extensions.has(a))
  );
}

export function* iterableProduct<T>(
  ...iterables: Iterable<T>[]
): IterableIterator<T[]> {
  if (iterables.length == 0) {
    yield [];
    return;
  }
  const [first, ...rest] = iterables;
  for (const item of first) {
    for (const productRest of iterableProduct(...rest)) {
      yield [item, ...productRest];
    }
  }
}
