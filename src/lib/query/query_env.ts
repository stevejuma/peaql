/* eslint-disable @typescript-eslint/no-explicit-any */
import { DateTime, DateTimeOptions, DateTimeUnit, Duration } from "luxon";
import { NotSupportedError, OperationalError } from "../errors";
import { Decimal, isValidNumber, NumberSource } from "../decimal";

import {
  EvalNode,
  getValueByDotNotation,
  isEqual,
  isNull,
  ASTERISK,
  Operation,
  typeOf,
  typeName,
  VARARG,
  DType,
  INTEGER,
  isSameType,
  typeCast,
  parseDateTime,
} from "./types";

import {
  EvalAggregator,
  EvalBetween,
  registerOperator,
  unaryOp,
  binaryOp,
  createFunction,
  createAggregatorFunction,
  AggregatorState,
  EvalQuery,
} from "./nodes";
import { parseDuration } from "../parser";

registerOperator(
  "BETWEEN",
  new Operation(
    [Number, Number, Number],
    Boolean,
    (operand: number, lower: number, upper: number) => {
      return lower <= operand && operand <= upper;
    },
    (operand: EvalNode, lower: EvalNode, upper: EvalNode) => {
      return new EvalBetween(operand, lower, upper);
    },
  ),
  new Operation(
    [DateTime, DateTime, DateTime],
    Boolean,
    (operand: DateTime, lower: DateTime, upper: DateTime) => {
      return (
        lower.toMillis() <= operand.toMillis() &&
        operand.toMillis() <= upper.toMillis()
      );
    },
    (operand: EvalNode, lower: EvalNode, upper: EvalNode) => {
      return new EvalBetween(operand, lower, upper);
    },
  ),
  new Operation(
    [Duration, Duration, Duration],
    Boolean,
    (operand: Duration, lower: Duration, upper: Duration) => {
      return (
        lower.toMillis() <= operand.toMillis() &&
        operand.toMillis() <= upper.toMillis()
      );
    },
    (operand: EvalNode, lower: EvalNode, upper: EvalNode) => {
      return new EvalBetween(operand, lower, upper);
    },
  ),
  new Operation(
    [Object, Object, Object],
    Boolean,
    (operand: any, lower: any, upper: any) => {
      return lower <= operand && operand <= upper;
    },
    (operand: EvalNode, lower: EvalNode, upper: EvalNode) => {
      return new EvalBetween(operand, lower, upper);
    },
  ),
);

registerOperator(
  "NOTBETWEEN",
  new Operation(
    [Number, Number, Number],
    Boolean,
    (operand: number, lower: number, upper: number) => {
      return lower <= operand && operand <= upper;
    },
    (operand: EvalNode, lower: EvalNode, upper: EvalNode) => {
      return new EvalBetween(operand, lower, upper, true);
    },
  ),
  new Operation(
    [DateTime, DateTime, DateTime],
    Boolean,
    (operand: number, lower: number, upper: number) => {
      return lower <= operand && operand <= upper;
    },
    (operand: EvalNode, lower: EvalNode, upper: EvalNode) => {
      return new EvalBetween(operand, lower, upper, true);
    },
  ),
);

unaryOp("NEGATE", negate, [INTEGER], INTEGER);
unaryOp("NEGATE", negate, [Decimal], Decimal);
unaryOp("NEGATE", negate, [Number], Number);
unaryOp("NEGATE", negate, [Duration], Duration);
unaryOp("-", negate, [INTEGER], INTEGER);
unaryOp("-", negate, [Decimal], Decimal);
unaryOp("-", negate, [Number], Number);
unaryOp("-", negate, [Duration], Duration);
function negate(a: unknown) {
  if (typeof a === "number") {
    return a * -1;
  } else if (a instanceof Decimal) {
    return a.neg();
  } else if (a instanceof Duration) {
    return a.mapUnits((v) => v * -1);
  }
  return null;
}

unaryOp("ISNULL", (a: any) => isNull(a), [Object], Boolean, true);
unaryOp("ISNOTNULL", (a: any) => !isNull(a), [Object], Boolean, true);
unaryOp(
  "NOT",
  (a: any) => {
    return a ? false : true;
  },
  [Object],
  Boolean,
  true,
);

unaryOp("+", positive, [INTEGER], INTEGER);
unaryOp("+", positive, [Number], Number);
unaryOp("+", positive, [Decimal], Decimal);
unaryOp("+", positive, [Duration], Duration);
function positive(a: unknown) {
  if (a instanceof Duration) {
    return a.mapUnits((v) => Math.abs(v));
  }
  return a;
}

binaryOp("OR", binaryConcat, [Number, Number], String);
binaryOp("OR", binaryConcat, [Number, String], String);
binaryOp("OR", binaryConcat, [String, Number], String);
binaryOp("OR", binaryConcat, [Number, String], String);
binaryOp("OR", binaryConcat, [String, String], String);
function binaryConcat(a: any, b: any) {
  return a.toString() + b.toString();
}

binaryOp("-", minus, [INTEGER, INTEGER], INTEGER);
binaryOp("-", minus, [Number, Number], Number);
binaryOp("-", minus, [Number, Decimal], Decimal);
binaryOp("-", minus, [Decimal, Number], Decimal);
binaryOp("-", minus, [Decimal, Decimal], Decimal);
binaryOp("-", minus, [DateTime, DateTime], Number);
binaryOp("-", minus, [DateTime, Number], DateTime);
binaryOp("-", minus, [Number, DateTime], DateTime);
binaryOp("-", minus, [Duration, Duration], Duration);
binaryOp("-", minus, [Duration, Number], Duration);
binaryOp("-", minus, [Number, Duration], Duration);
binaryOp("-", minus, [Duration, DateTime], DateTime);
binaryOp("-", minus, [DateTime, Duration], DateTime);
export function minus(a: any, b: any) {
  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  } else if (typeof a === "number" && b instanceof Decimal) {
    return new Decimal(a).minus(b);
  } else if (a instanceof Decimal && typeof b === "number") {
    return a.minus(b);
  } else if (a instanceof Decimal && b instanceof Decimal) {
    return a.minus(b);
  } else if (a instanceof DateTime && b instanceof DateTime) {
    return a.diff(b).days;
  } else if (a instanceof DateTime && typeof b === "number") {
    return a.minus({ days: b });
  } else if (typeof a === "number" && b instanceof DateTime) {
    return b.minus({ days: a });
  } else if (a instanceof Duration && b instanceof Duration) {
    return a.minus(b);
  } else if (a instanceof Duration && typeof b === "number") {
    return a.mapUnits((n) => n - b);
  } else if (typeof a === "number" && b instanceof Duration) {
    return b.mapUnits((n) => n - a);
  } else if (a instanceof Duration && b instanceof DateTime) {
    return b.minus(a);
  } else if (a instanceof DateTime && b instanceof Duration) {
    return a.minus(b);
  }
  return a - b || null;
}

binaryOp("+", plus, [INTEGER, INTEGER], INTEGER);
binaryOp("+", plus, [Number, Number], Number);
binaryOp("+", plus, [Number, Decimal], Decimal);
binaryOp("+", plus, [Decimal, Number], Decimal);
binaryOp("+", plus, [Decimal, Decimal], Decimal);
binaryOp("+", plus, [DateTime, Number], DateTime);
binaryOp("+", plus, [Number, DateTime], DateTime);
binaryOp("+", plus, [Duration, Duration], Duration);
binaryOp("+", plus, [Duration, Number], Duration);
binaryOp("+", plus, [Number, Duration], Duration);
binaryOp("+", plus, [Duration, DateTime], DateTime);
binaryOp("+", plus, [DateTime, Duration], DateTime);
binaryOp("+", plus, [Number, Number], Number);
binaryOp("+", plus, [Number, String], String);
binaryOp("+", plus, [String, Number], String);
binaryOp("+", plus, [String, String], String);
export function plus(a: any, b: any) {
  if (typeof a === "number" && typeof b === "number") {
    return a + b;
  } else if (typeof a === "number" && b instanceof Decimal) {
    return new Decimal(a).plus(b);
  } else if (a instanceof Decimal && typeof b === "number") {
    return a.plus(b);
  } else if (a instanceof Decimal && b instanceof Decimal) {
    return a.plus(b);
  } else if (a instanceof DateTime && typeof b === "number") {
    return a.plus({ days: b });
  } else if (typeof a === "number" && b instanceof DateTime) {
    return b.plus({ days: a });
  } else if (a instanceof Duration && b instanceof Duration) {
    return a.plus(b);
  } else if (a instanceof Duration && typeof b === "number") {
    return a.mapUnits((n) => n + b);
  } else if (typeof a === "number" && b instanceof Duration) {
    return b.mapUnits((n) => n + a);
  } else if (a instanceof Duration && b instanceof DateTime) {
    return b.plus(a);
  } else if (a instanceof DateTime && b instanceof Duration) {
    return a.plus(b);
  }
  return a + b;
}

binaryOp("*", multiply, [INTEGER, INTEGER], INTEGER);
binaryOp("*", multiply, [Number, Number], Number);
binaryOp("*", multiply, [Number, Decimal], Decimal);
binaryOp("*", multiply, [Decimal, Number], Decimal);
binaryOp("*", multiply, [Decimal, Decimal], Decimal);
binaryOp("*", multiply, [Duration, Number], Duration);
binaryOp("*", multiply, [Number, Duration], Duration);
export function multiply(a: any, b: any) {
  if (typeof a === "number" && typeof b === "number") {
    return a * b;
  } else if (typeof a === "number" && b instanceof Decimal) {
    return new Decimal(a).mul(b);
  } else if (a instanceof Decimal && typeof b === "number") {
    return a.mul(b);
  } else if (a instanceof Decimal && b instanceof Decimal) {
    return a.mul(b);
  } else if (a instanceof Duration && typeof b === "number") {
    return a.mapUnits((n) => n * b);
  } else if (typeof a === "number" && b instanceof Duration) {
    return b.mapUnits((n) => n * a);
  }
  return null;
}
binaryOp("/", divideInt, [INTEGER, INTEGER], INTEGER);
export function divideInt(a: number, b: number) {
  if (b == 0) {
    return null;
  }
  const value = Math.trunc(a / b);
  if (value === 0) {
    return 0;
  }
  return value;
}

binaryOp("/", divide, [Number, Number], Number);
binaryOp("/", divide, [Number, Decimal], Decimal);
binaryOp("/", divide, [Decimal, Number], Decimal);
binaryOp("/", divide, [Decimal, Decimal], Decimal);
binaryOp("/", divide, [Number, Duration], Duration);
binaryOp("/", divide, [Duration, Number], Duration);
export function divide(a: any, b: any) {
  if (typeof a === "number" && typeof b === "number") {
    return b == 0 ? null : a / b;
  } else if (typeof a === "number" && b instanceof Decimal) {
    return b.zero ? null : new Decimal(a).div(b);
  } else if (a instanceof Decimal && typeof b === "number") {
    return b === 0 ? null : a.div(b);
  } else if (a instanceof Decimal && b instanceof Decimal) {
    return b.zero ? null : a.div(b);
  } else if (a instanceof Duration && typeof b === "number") {
    return a.mapUnits((n) => n / b);
  } else if (typeof a === "number" && b instanceof Duration) {
    return b.mapUnits((n) => n / a);
  }
  return null;
}

binaryOp("%", mod, [Number, Number], Number);
binaryOp("%", mod, [Number, Decimal], Decimal);
binaryOp("%", mod, [Decimal, Number], Decimal);
binaryOp("%", mod, [Decimal, Decimal], Decimal);
export function mod(a: any, b: any) {
  if (typeof a === "number" && typeof b === "number") {
    return b == 0 ? null : a % b;
  } else if (typeof a === "number" && b instanceof Decimal) {
    return b.zero ? null : new Decimal(a % b.number);
  } else if (a instanceof Decimal && typeof b === "number") {
    return b === 0 ? null : new Decimal(a.number % b);
  } else if (a instanceof Decimal && b instanceof Decimal) {
    return b.zero ? null : new Decimal(a.number % b.number);
  }
  return null;
}

binaryOp("~", match, [String, String], Boolean);
binaryOp("~*", (a, b) => match(a, b, "ig"), [String, String], Boolean);
binaryOp("!~", (a, b) => !match(a, b), [String, String], Boolean);
binaryOp("!~*", (a, b) => !match(a, b, "ig"), [String, String], Boolean);
export function match(a: string, b: string, flags: string = "ig") {
  return new RegExp(b, flags).test(a);
}

binaryOp("?~", matches, [String, String], Boolean);
binaryOp("?~*", (a, b) => matches(a, b, "ig"), [String, String], Boolean);
binaryOp("?~", matches, [String, String, String], Boolean);
export function matches(a: string, b: string, flags: string = "g") {
  const match = /^(\s*\(\s*\?\s*([^)]+)\s*\))(.*)/.exec(a);
  if (match && match[2] && match[3]) {
    return new RegExp(match[3], match[2]).test(b);
  }
  return new RegExp(a, flags).test(b);
}

binaryOp("IN", in_, [Object, Set], Boolean);
binaryOp("IN", in_, [Object, Object], Boolean);
binaryOp("IN", in_, [Object, [Object]], Boolean);
binaryOp("NOTIN", (a, b) => !in_(a, b), [Object, Set], Boolean);
binaryOp("NOTIN", (a, b) => !in_(a, b), [Object, Object], Boolean);
binaryOp("NOTIN", (a, b) => !in_(a, b), [Object, [Object]], Boolean);
export function in_(a: any, b: any): boolean {
  if (b instanceof Set) {
    return in_(a, [...b]);
  } else if (Array.isArray(b)) {
    return b.flat().some((it) => isEqual(a, it));
  }
  return false;
}

binaryOp(">", gt, [String, DateTime], Boolean);
binaryOp(">", gt, [String, Duration], Boolean);
binaryOp(">", gt, [Number, Number], Boolean);
binaryOp(">", gt, [Number, DateTime], Boolean);
binaryOp(">", gt, [Number, Decimal], Boolean);
binaryOp(">", gt, [Number, Duration], Boolean);
binaryOp(">", gt, [Decimal, Number], Boolean);
binaryOp(">", gt, [Decimal, DateTime], Boolean);
binaryOp(">", gt, [Decimal, Decimal], Boolean);
binaryOp(">", gt, [Decimal, Duration], Boolean);
binaryOp(">", gt, [DateTime, DateTime], Boolean);
binaryOp(">", gt, [DateTime, String], Boolean);
binaryOp(">", gt, [DateTime, Number], Boolean);
binaryOp(">", gt, [DateTime, Decimal], Boolean);
binaryOp(">", gt, [Duration, Duration], Boolean);
binaryOp(">", gt, [Duration, String], Boolean);
binaryOp(">", gt, [Duration, Number], Boolean);
binaryOp(">", gt, [Duration, Decimal], Boolean);
export function gt(a: any, b: any) {
  if (typeof a === "number" && typeof b === "number") {
    return a > b;
  } else if (typeof a === "number" && b instanceof Decimal) {
    return new Decimal(a).gt(b);
  } else if (a instanceof Decimal && typeof b === "number") {
    return a.gt(b);
  } else if (a instanceof Decimal && b instanceof Decimal) {
    return a.gt(b);
  } else if (a instanceof DateTime && b instanceof DateTime) {
    return a.toMillis() > b.toMillis();
  } else if (a instanceof Duration && b instanceof Duration) {
    return a.toMillis() >= b.toMillis();
  } else if (
    [a, b].every(
      (v) =>
        v instanceof DateTime ||
        ["number", "string"].includes(typeof v) ||
        v instanceof Decimal,
    )
  ) {
    const aDt: DateTime | null =
      a instanceof DateTime ? a : (typeCast(a, DateTime) as DateTime);
    const bDt: DateTime | null =
      b instanceof DateTime ? b : (typeCast(b, DateTime) as DateTime);

    if (!aDt || !bDt) {
      throw new NotSupportedError(
        `Unsupported operator(>): (${typeName(a)} > ${typeName(b)})`,
      );
    }
    return aDt.toMillis() > bDt.toMillis();
  } else if (
    [a, b].every(
      (v) =>
        v instanceof Duration ||
        ["number", "string"].includes(typeof v) ||
        v instanceof Decimal,
    )
  ) {
    const aDt: Duration | null =
      a instanceof Duration ? a : (typeCast(a, Duration) as Duration);
    const bDt: Duration | null =
      b instanceof Duration ? b : (typeCast(b, Duration) as Duration);

    if (!aDt || !bDt) {
      throw new NotSupportedError(
        `Unsupported operator(>): (${typeName(a)} > ${typeName(b)})`,
      );
    }
    return aDt.toMillis() > bDt.toMillis();
  }
  return null;
}

binaryOp(">=", gte, [String, DateTime], Boolean);
binaryOp(">=", gte, [String, Duration], Boolean);
binaryOp(">=", gte, [Number, Number], Boolean);
binaryOp(">=", gte, [Number, DateTime], Boolean);
binaryOp(">=", gte, [Number, Decimal], Boolean);
binaryOp(">=", gte, [Number, Duration], Boolean);
binaryOp(">=", gte, [Decimal, Number], Boolean);
binaryOp(">=", gte, [Decimal, DateTime], Boolean);
binaryOp(">=", gte, [Decimal, Decimal], Boolean);
binaryOp(">=", gte, [Decimal, Duration], Boolean);
binaryOp(">=", gte, [DateTime, DateTime], Boolean);
binaryOp(">=", gte, [DateTime, String], Boolean);
binaryOp(">=", gte, [DateTime, Number], Boolean);
binaryOp(">=", gte, [DateTime, Decimal], Boolean);
binaryOp(">=", gte, [Duration, Duration], Boolean);
binaryOp(">=", gte, [Duration, String], Boolean);
binaryOp(">=", gte, [Duration, Number], Boolean);
binaryOp(">=", gte, [Duration, Decimal], Boolean);
export function gte(a: any, b: any) {
  if (typeof a === "number" && typeof b === "number") {
    return a >= b;
  } else if (typeof a === "number" && b instanceof Decimal) {
    return new Decimal(a).gte(b);
  } else if (a instanceof Decimal && typeof b === "number") {
    return a.gte(b);
  } else if (a instanceof Decimal && b instanceof Decimal) {
    return a.gte(b);
  } else if (a instanceof DateTime && b instanceof DateTime) {
    return a.toMillis() >= b.toMillis();
  } else if (a instanceof Duration && b instanceof Duration) {
    return a.toMillis() >= b.toMillis();
  } else if (
    [a, b].every(
      (v) =>
        v instanceof DateTime ||
        ["number", "string"].includes(typeof v) ||
        v instanceof Decimal,
    )
  ) {
    const aDt: DateTime | null =
      a instanceof DateTime ? a : (typeCast(a, DateTime) as DateTime);
    const bDt: DateTime | null =
      b instanceof DateTime ? b : (typeCast(b, DateTime) as DateTime);

    if (!aDt || !bDt) {
      throw new NotSupportedError(
        `Unsupported operator(>): (${typeName(a)} > ${typeName(b)})`,
      );
    }
    return aDt.toMillis() >= bDt.toMillis();
  } else if (
    [a, b].every(
      (v) =>
        v instanceof Duration ||
        ["number", "string"].includes(typeof v) ||
        v instanceof Decimal,
    )
  ) {
    const aDt: Duration | null =
      a instanceof Duration ? a : (typeCast(a, Duration) as Duration);
    const bDt: Duration | null =
      b instanceof Duration ? b : (typeCast(b, Duration) as Duration);

    if (!aDt || !bDt) {
      throw new NotSupportedError(
        `Unsupported operator(>): (${typeName(a)} > ${typeName(b)})`,
      );
    }
    return aDt.toMillis() >= bDt.toMillis();
  }
  return null;
}

binaryOp("<", lt, [String, DateTime], Boolean);
binaryOp("<", lt, [String, Duration], Boolean);
binaryOp("<", lt, [Number, Number], Boolean);
binaryOp("<", lt, [Number, DateTime], Boolean);
binaryOp("<", lt, [Number, Decimal], Boolean);
binaryOp("<", lt, [Number, Duration], Boolean);
binaryOp("<", lt, [Decimal, Number], Boolean);
binaryOp("<", lt, [Decimal, DateTime], Boolean);
binaryOp("<", lt, [Decimal, Decimal], Boolean);
binaryOp("<", lt, [Decimal, Duration], Boolean);
binaryOp("<", lt, [DateTime, DateTime], Boolean);
binaryOp("<", lt, [DateTime, String], Boolean);
binaryOp("<", lt, [DateTime, Number], Boolean);
binaryOp("<", lt, [DateTime, Decimal], Boolean);
binaryOp("<", lt, [Duration, Duration], Boolean);
binaryOp("<", lt, [Duration, String], Boolean);
binaryOp("<", lt, [Duration, Number], Boolean);
binaryOp("<", lt, [Duration, Decimal], Boolean);
export function lt(a: any, b: any) {
  if (typeof a === "number" && typeof b === "number") {
    return a < b;
  } else if (typeof a === "number" && b instanceof Decimal) {
    return new Decimal(a).lt(b);
  } else if (a instanceof Decimal && typeof b === "number") {
    return a.lt(b);
  } else if (a instanceof Decimal && b instanceof Decimal) {
    return a.lt(b);
  } else if (a instanceof DateTime && b instanceof DateTime) {
    return a.toMillis() < b.toMillis();
  } else if (a instanceof Duration && b instanceof Duration) {
    return a.toMillis() < b.toMillis();
  } else if (
    [a, b].every(
      (v) =>
        v instanceof DateTime ||
        ["number", "string"].includes(typeof v) ||
        v instanceof Decimal,
    )
  ) {
    const aDt: DateTime | null =
      a instanceof DateTime ? a : (typeCast(a, DateTime) as DateTime);
    const bDt: DateTime | null =
      b instanceof DateTime ? b : (typeCast(b, DateTime) as DateTime);

    if (!aDt || !bDt) {
      throw new NotSupportedError(
        `Unsupported operator(>): (${typeName(a)} > ${typeName(b)})`,
      );
    }
    return aDt.toMillis() < bDt.toMillis();
  } else if (
    [a, b].every(
      (v) =>
        v instanceof Duration ||
        ["number", "string"].includes(typeof v) ||
        v instanceof Decimal,
    )
  ) {
    const aDt: Duration | null =
      a instanceof Duration ? a : (typeCast(a, Duration) as Duration);
    const bDt: Duration | null =
      b instanceof Duration ? b : (typeCast(b, Duration) as Duration);

    if (!aDt || !bDt) {
      throw new NotSupportedError(
        `Unsupported operator(>): (${typeName(a)} > ${typeName(b)})`,
      );
    }
    return aDt.toMillis() < bDt.toMillis();
  }
  return null;
}

binaryOp("<=", lte, [String, DateTime], Boolean);
binaryOp("<=", lte, [String, Duration], Boolean);
binaryOp("<=", lte, [Number, Number], Boolean);
binaryOp("<=", lte, [Number, DateTime], Boolean);
binaryOp("<=", lte, [Number, Decimal], Boolean);
binaryOp("<=", lte, [Number, Duration], Boolean);
binaryOp("<=", lte, [Decimal, Number], Boolean);
binaryOp("<=", lte, [Decimal, DateTime], Boolean);
binaryOp("<=", lte, [Decimal, Decimal], Boolean);
binaryOp("<=", lte, [Decimal, Duration], Boolean);
binaryOp("<=", lte, [DateTime, DateTime], Boolean);
binaryOp("<=", lte, [DateTime, String], Boolean);
binaryOp("<=", lte, [DateTime, Number], Boolean);
binaryOp("<=", lte, [DateTime, Decimal], Boolean);
binaryOp("<=", lte, [Duration, Duration], Boolean);
binaryOp("<=", lte, [Duration, String], Boolean);
binaryOp("<=", lte, [Duration, Number], Boolean);
binaryOp("<=", lte, [Duration, Decimal], Boolean);
export function lte(a: any, b: any) {
  if (typeof a === "number" && typeof b === "number") {
    return a <= b;
  } else if (typeof a === "number" && b instanceof Decimal) {
    return new Decimal(a).lte(b);
  } else if (a instanceof Decimal && typeof b === "number") {
    return a.lte(b);
  } else if (a instanceof Decimal && b instanceof Decimal) {
    return a.lte(b);
  } else if (a instanceof DateTime && b instanceof DateTime) {
    return a.toMillis() <= b.toMillis();
  } else if (a instanceof Duration && b instanceof Duration) {
    return a.toMillis() <= b.toMillis();
  } else if (
    [a, b].every(
      (v) =>
        v instanceof DateTime ||
        ["number", "string"].includes(typeof v) ||
        v instanceof Decimal,
    )
  ) {
    const aDt: DateTime | null =
      a instanceof DateTime ? a : (typeCast(a, DateTime) as DateTime);
    const bDt: DateTime | null =
      b instanceof DateTime ? b : (typeCast(b, DateTime) as DateTime);

    if (!aDt || !bDt) {
      throw new NotSupportedError(
        `Unsupported operator(>): (${typeName(a)} > ${typeName(b)})`,
      );
    }
    return aDt.toMillis() <= bDt.toMillis();
  } else if (
    [a, b].every(
      (v) =>
        v instanceof Duration ||
        ["number", "string"].includes(typeof v) ||
        v instanceof Decimal,
    )
  ) {
    const aDt: Duration | null =
      a instanceof Duration ? a : (typeCast(a, Duration) as Duration);
    const bDt: Duration | null =
      b instanceof Duration ? b : (typeCast(b, Duration) as Duration);

    if (!aDt || !bDt) {
      throw new NotSupportedError(
        `Unsupported operator(>): (${typeName(a)} > ${typeName(b)})`,
      );
    }
    return aDt.toMillis() <= bDt.toMillis();
  }
  return null;
}

binaryOp("=", isEqual, [Number, Number], Boolean);
binaryOp("=", isEqual, [Boolean, Boolean], Boolean);
binaryOp("=", isEqual, [Number, Decimal], Boolean);
binaryOp("=", isEqual, [Decimal, Number], Boolean);
binaryOp("=", isEqual, [Decimal, Decimal], Boolean);
binaryOp("=", isEqual, [Duration, Duration], Boolean);
binaryOp("=", isEqual, [String, String], Boolean);

binaryOp("!=", (a, b) => !isEqual(a, b), [Number, Number], Boolean);
binaryOp("!=", (a, b) => !isEqual(a, b), [Boolean, Boolean], Boolean);
binaryOp("!=", (a, b) => !isEqual(a, b), [Number, Decimal], Boolean);
binaryOp("!=", (a, b) => !isEqual(a, b), [Decimal, Number], Boolean);
binaryOp("!=", (a, b) => !isEqual(a, b), [Decimal, Decimal], Boolean);
binaryOp("!=", (a, b) => !isEqual(a, b), [Duration, Duration], Boolean);
binaryOp("!=", (a, b) => !isEqual(a, b), [String, String], Boolean);

createFunction("getitem", [Object, String], Object, getItem);
createFunction("getitem", [Object, String, Object], Object, getItem);
export function getItem(obj: any, key: string, fallback: any = null) {
  return (
    getValueByDotNotation(obj, key) ?? getValueByDotNotation(fallback, key)
  );
}

// Query env

// Type Casting
createFunction("bool", [Object], Boolean, (a: any) => Boolean(a));
createFunction(
  typeName(Boolean),
  [Object],
  Boolean,
  (a: any) => a === "null" || Boolean(a),
);
createFunction("int", [Number], Number, toInt);
createFunction("int", [Object], Number, toInt);
createFunction("integer", [Object], INTEGER, toInt);
function toInt(a: unknown): number | null {
  if (isNull(a)) {
    return null;
  }
  const value = toDecimal(a);
  return isNull(value) ? null : Math.trunc(value.number);
}

createFunction("real", [INTEGER], INTEGER, toReal);
createFunction("real", [Object], Number, toReal);
createFunction("integer", [Object], Number, toReal);
function toReal(a: unknown): number | null {
  if (isNull(a)) {
    return null;
  }
  return toDecimal(a)?.number ?? null;
}

createFunction("interval", [String], Duration, toInterval);
createFunction("interval", [Duration], Duration, toInterval);
function toInterval(a: unknown): Duration {
  if (a instanceof Duration) {
    return a;
  } else if (typeof a === "string") {
    return parseDuration(a);
  }
}

createFunction("concat", [String, VARARG, String], String, concat);
function concat(...a: string[]): string {
  return a.filter((it) => !isNull(it)).join("");
}

createFunction("timestamp", [String], DateTime, toTimestamp);
createFunction("timestamp", [Number], DateTime, toTimestamp);
createFunction("timestamp", [DateTime], DateTime, toTimestamp);
function toTimestamp(a: unknown): DateTime {
  if (a instanceof DateTime) {
    return a;
  } else if (typeof a === "string") {
    return [DateTime.fromSQL(a), DateTime.fromISO(a)].find((it) => it.isValid);
  } else if (typeof a === "number") {
    return DateTime.fromMillis(a);
  } else if (a instanceof Date) {
    return DateTime.fromJSDate(a);
  }
}

createFunction("to_char", [DateTime, String], String, toChar);
createFunction("to_char", [Duration, String], String, toChar);
createFunction("to_char", [Number, String], String, toChar);
createFunction("to_char", [Decimal, String], String, toChar);
function toChar(a: unknown, format?: string): string {
  if (a instanceof DateTime) {
    return format ? a.toFormat(format) : a.toSQL();
  } else if (a instanceof Duration) {
    return format
      ? a.toFormat(format)
      : a.toHuman({ listStyle: "long" }).replace(",", "");
  } else if (typeof a === "number") {
    return format ? toCharNumber(a, format) : a.toString();
  } else if (a instanceof Decimal) {
    return format ? toChar(a.number, format) : a.toString();
  }
  return null;
}

/**
 * Formats a number according to a PostgreSQL-style TO_CHAR format.
 *
 * @param number The number to format.
 * @param format The format string (e.g., '999,999.99', '000.00', '$999.99').
 * @returns The formatted number as a string.
 */
function toCharNumber(number: number, format: string): string {
  const isNegative = number < 0;
  number = Math.abs(number);

  const decimalSeparator = ".";
  const thousandSeparator = ",";
  let currencySymbol = "";
  let useScientific = false;
  let formatPattern = format.trim();

  // Detect scientific notation
  if (formatPattern.includes("EEEE")) {
    useScientific = true;
    formatPattern = formatPattern.replace("EEEE", "");
  }

  // Detect currency symbols ($, €, £)
  if (formatPattern.startsWith("$")) {
    currencySymbol = "$";
    formatPattern = formatPattern.slice(1);
  } else if (formatPattern.startsWith("€")) {
    currencySymbol = "€";
    formatPattern = formatPattern.slice(1);
  } else if (formatPattern.startsWith("£")) {
    currencySymbol = "£";
    formatPattern = formatPattern.slice(1);
  }

  // Split integer and decimal parts of the format
  const [intFormat, decFormat] = formatPattern.split(decimalSeparator);

  // Determine decimal places and rounding
  const decimalPlaces = decFormat ? decFormat.length : 0;
  const formattedNumber = useScientific
    ? number.toExponential(decimalPlaces)
    : number.toFixed(decimalPlaces);

  // Split into integer and decimal parts
  const [intPart, decPart = ""] = formattedNumber.split(".");

  // Apply zero-padding or space-padding for integer part
  let intFormatted = "";
  let intIndex = intFormat.length - 1;
  let numIndex = intPart.length - 1;

  while (intIndex >= 0) {
    const fmtChar = intFormat[intIndex];

    if (fmtChar === "0") {
      intFormatted = (numIndex >= 0 ? intPart[numIndex] : "0") + intFormatted;
      numIndex--;
    } else if (fmtChar === "9") {
      intFormatted = (numIndex >= 0 ? intPart[numIndex] : " ") + intFormatted;
      numIndex--;
    } else if (fmtChar === thousandSeparator) {
      intFormatted = (numIndex >= 0 ? thousandSeparator : "") + intFormatted;
    } else {
      intFormatted = fmtChar + intFormatted;
    }

    intIndex--;
  }

  // Apply decimal formatting
  const decFormatted = decFormat
    ? decimalSeparator + (decPart || "0").padEnd(decimalPlaces, "0")
    : "";

  // Construct final result
  let result = `${currencySymbol}${intFormatted}${decFormatted}`;

  // Handle negative numbers
  if (isNegative) result = `-${result.trim()}`;

  return result.trim();
}

createFunction("timestamptz", [String], DateTime, toTimestamptz);
createFunction("timestamptz", [String, String], DateTime, toTimestamptz);
createFunction("timestamptz", [Number], DateTime, toTimestamptz);
createFunction("timestamptz", [Number, String], DateTime, toTimestamptz);
createFunction("timestamptz", [DateTime], DateTime, toTimestamptz);
createFunction("timestamptz", [DateTime, String], DateTime, toTimestamptz);
function toTimestamptz(a: unknown, zone?: string): DateTime {
  const opts: DateTimeOptions = zone ? { zone } : { setZone: true };
  if (a instanceof DateTime) {
    return zone ? a.setZone(zone) : a;
  } else if (typeof a === "string") {
    return [DateTime.fromSQL(a, opts), DateTime.fromISO(a, opts)].find(
      (it) => it.isValid,
    );
  } else if (typeof a === "number") {
    return DateTime.fromMillis(a, opts);
  } else if (a instanceof Date) {
    return DateTime.fromJSDate(a);
  }
}

createFunction(typeName(Number), [Object], Number, toNumber);
export function toNumber(a: unknown): number | null {
  const value = toDecimal(a);
  return isNull(value) ? null : value.number;
}

createFunction(typeName(Decimal), [Object], Decimal, toDecimal);
function toDecimal(a: unknown): Decimal | null {
  if (isNull(a)) {
    return null;
  }
  if (typeof a === "number") {
    return new Decimal(a);
  } else if (typeof a === "string") {
    if (isValidNumber(a)) {
      return new Decimal(a);
    }
    const clean: string = a.replaceAll(/[^0-9.]+/g, "");
    if (isValidNumber(clean)) {
      return new Decimal(clean);
    }
  } else if (a === true) {
    return Decimal.ONE;
  } else if (a === false) {
    return Decimal.ZERO;
  } else if (a instanceof Decimal) {
    return a;
  }
  return null;
}

createFunction("text", [Object], String, toString);
createFunction(typeName(String), [Object], String, toString);
function toString(a: any): string {
  if (isNull(a)) {
    return null;
  }
  if (a === true) return "TRUE";
  if (a === false) return "FALSE";
  return a?.toString();
}

createFunction(typeName(DateTime), [String], DateTime, toDate);
createFunction(typeName(DateTime), [Number], DateTime, toDate);
createFunction(typeName(DateTime), [DateTime], DateTime, toDate);
createFunction(typeName(DateTime), [Decimal], DateTime, toDate);
function toDate(a: any): DateTime {
  if (a instanceof DateTime) {
    return a;
  } else if (typeof a === "string") {
    return parseDateTime(a);
  } else if (a instanceof Date) {
    return DateTime.fromJSDate(a);
  } else if (typeof a === "number") {
    return DateTime.fromMillis(a);
  } else if (a instanceof Decimal) {
    return DateTime.fromMillis(a.number);
  }
  return null;
}

// Functions

createFunction("safediv", [Number, Number], Decimal, safeDiv, true);
createFunction("safediv", [Number, Decimal], Decimal, safeDiv, true);
createFunction("safediv", [Decimal, Decimal], Decimal, safeDiv, true);
createFunction("safediv", [Decimal, Number], Decimal, safeDiv, true);
function safeDiv(a: unknown, b: unknown) {
  const valueA = toDecimal(a);
  const valueB = toDecimal(b);
  if (isNull(valueA) || isNull(valueB)) {
    return null;
  }
  if (valueB.zero) {
    return Decimal.ZERO;
  }
  return valueA.div(valueB);
}

createFunction("abs", [Number], Decimal, abs, true);
createFunction("abs", [Decimal], Decimal, abs, true);
function abs(a: unknown) {
  return toDecimal(a)?.abs() ?? null;
}

createFunction("round", [Number], Decimal, round, true);
createFunction("round", [Number, Number], Decimal, round, true);
createFunction("round", [Number, Decimal], Decimal, round, true);
createFunction("round", [Decimal, Decimal], Decimal, round, true);
createFunction("round", [Decimal, Number], Decimal, round, true);
function round(value: number, digits: number = 2): Decimal {
  return toDecimal(value)?.toScale(digits) ?? null;
}

createFunction("toFixed", [Number, Number], String, toFixed);
createFunction("toFixed", [Decimal, Number], String, toFixed);
function toFixed(value: number | Decimal, digits: number): string {
  return toDecimal(value)?.toFixed(digits);
}

createFunction("length", [Set], Number, length, true);
createFunction("length", [String], Number, length, true);
createFunction("length", [[Object]], Number, length, true);
function length(value: unknown): number | null {
  if (typeof value === "string") {
    return value.length;
  } else if (value instanceof Set) {
    return value.size;
  } else if (Array.isArray(value)) {
    return value.length;
  }
  return null;
}

createFunction("maxwidth", [String, Number], String, maxWidth, true);
function maxWidth(x: string, n: number) {
  if (x.length <= n) {
    return x;
  }
  return x.slice(0, n) + "...";
}

createFunction("substr", [String, Number, Number], String, subString, true);
createFunction("substr", [String, Number], String, subString, true);
function subString(str: string, start: number, end?: number) {
  return str.substring(start, end);
}

createFunction("splitcomp", [String, String, Number], String, splitComp, true);
function splitComp(str: string, delim: string, index: number) {
  return str.split(delim)[index] ?? null;
}

// Operations on dates
createFunction("year", [DateTime], Number, year, true);
function year(date: DateTime) {
  return date.year;
}

createFunction("month", [DateTime], Number, month, true);
function month(date: DateTime) {
  return date.month;
}

createFunction("day", [DateTime], Number, day, true);
function day(date: DateTime) {
  return date.month;
}

createFunction("yearmonth", [DateTime], DateTime, yearMonth, true);
function yearMonth(date: DateTime) {
  return date.startOf("month");
}

createFunction("quarter", [DateTime], String, quarter, true);
function quarter(date: DateTime) {
  return date.toFormat("yyyy-'Q'Q");
}

createFunction("weekday", [DateTime], String, weekday, true);
function weekday(date: DateTime) {
  return date.weekdayShort;
}

createFunction("today", [], DateTime, today, true);
createFunction("now", [], DateTime, today, true);
function today() {
  return DateTime.now();
}

createFunction("meta", [Object], Object, entryMeta, true);
createFunction("entry_meta", [Object], Object, entryMeta, true);
createFunction("any_meta", [Object], Object, entryMeta, true);
function entryMeta(context: any, key: string) {
  return getValueByDotNotation(context, key);
}

createFunction("grep", [String, String], String, grep, true);
function grep(pattern: string, input: string) {
  const match = new RegExp(pattern).exec(input) || [];
  return match[0] ?? null;
}

createFunction("grepn", [String, String, Number], String, grepn, true);
function grepn(pattern: string, input: string, index: number) {
  const match = new RegExp(pattern).exec(input) || [];
  return match[index] ?? null;
}

createFunction("format", [String, VARARG, Object], String, sprintf, true);
function sprintf(format: string, ...args: any[]): string {
  let argIndex = 0;
  return format.replace(
    /%%|%([-+0]?)(\d*|\*)\.?(\d*)([dfsx])/g,
    (match, flag, width, precision, type) => {
      if (match === "%%") {
        return "%";
      }

      if (argIndex >= args.length) {
        throw new Error("Too few arguments for format string");
      }
      if (width === "*") {
        width = args[argIndex++];
      }
      const value = args[argIndex++];
      let formatted: string;

      switch (type) {
        case "d": // Integer
          formatted = parseInt(value, 10).toString();
          break;
        case "f": // Floating-point
          formatted = parseFloat(value).toFixed(
            precision ? parseInt(precision, 10) : 6,
          );
          break;
        case "s": // String
          formatted = String(value);
          break;
        case "x": // Hexadecimal
          formatted = parseInt(value, 10).toString(16);
          break;
        default:
          throw new Error(`Unsupported format specifier: ${type}`);
      }

      // Handle width and padding
      const padChar = flag === "0" ? "0" : " ";

      const declaredWidth = width ? parseInt(width, 10) : 0;
      if (declaredWidth < 0) {
        flag = "-";
      }
      const minWidth = Math.abs(declaredWidth);
      if (formatted.length < minWidth) {
        formatted =
          flag === "-"
            ? formatted.padEnd(minWidth, " ")
            : formatted.padStart(minWidth, padChar);
      }

      return formatted;
    },
  );
}

createFunction("subst", [String, String, Number], String, subst, true);
function subst(pattern: string, input: string, replace: string) {
  const regex = new RegExp(pattern, "g");
  return input.replaceAll(regex, replace);
}

createFunction("upper", [String], String, upper, true);
function upper(str: string) {
  return str.toUpperCase();
}

createFunction("lower", [String], String, lower, true);
function lower(str: string) {
  return str.toLocaleLowerCase();
}

createFunction("findFirst", [String, [String]], String, findFirst, true);
createFunction("findFirst", [String, Set], String, findFirst, true);
function findFirst(pattern: string, input: string[] | Set<string>) {
  const regex = new RegExp(pattern, "g");
  return [...input].find((it) => regex.test(it));
}

createFunction("joinstr", [Set, [Object]], String, join, true);
function join(x: Set<unknown> | unknown[]) {
  return [...x].join(", ");
}

createFunction("repr", [Object], String, repr, true);
function repr(obj: unknown) {
  return obj.toString();
}

createFunction("empty", [Object], Boolean, empty, true);
function empty(obj: unknown) {
  if (typeof obj === "string") {
    return obj.length == 0;
  } else if (Array.isArray(obj)) {
    return obj.length == 0;
  } else if (typeof obj === "number") {
    return obj === 0;
  } else if (obj instanceof Decimal) {
    return obj.zero;
  } else if (obj instanceof Set) {
    return obj.size == 0;
  }
  return null;
}

createFunction("parse_date", [String], DateTime, parseDate, true);
createFunction("parse_date", [String, String], DateTime, parseDate, true);
function parseDate(date: string, format: string = "") {
  if (!format) {
    return parseDateTime(date);
  }
  return DateTime.fromFormat(date, format);
}

createFunction("date_diff", [DateTime, DateTime], Number, dateDiff, true);
function dateDiff(a: DateTime, b: DateTime) {
  return a.diff(b).days;
}

createFunction("date_add", [DateTime, Number], Number, dateAdd, true);
function dateAdd(a: DateTime, days: number) {
  return a.plus({ days });
}

createFunction("date_trunc", [String, DateTime], DateTime, dateTrunc, true);
createFunction("date_start", [String, DateTime], DateTime, dateTrunc, true);
function dateTrunc(field: DateTimeUnit, date: DateTime) {
  return date.startOf(field);
}

createFunction("date_end", [String, DateTime], DateTime, dateEnd, true);
createFunction("date_trunc_end", [String, DateTime], DateTime, dateEnd, true);
function dateEnd(field: DateTimeUnit, date: DateTime) {
  return date.endOf(field);
}

createFunction("date_part", [String, DateTime], DateTime, datePart, true);
function datePart(field: string, date: DateTime): number {
  switch (field) {
    case "weekday":
    case "dow":
      return date.localWeekday;
    case "isoweekday":
    case "isodow":
      return date.weekday;
    case "week":
      return date.weekNumber;
    case "month":
      return date.month;
    case "quarter":
      return date.quarter;
    case "year":
      return date.year;
    case "isoyear":
      return date.weekYear;
    case "decade":
      return date.year - (date.year % 10);
    case "century":
      return date.year - 1 - ((date.year - 1) % 100) + 1;
    case "millennium":
      return date.year - 1 - ((date.year - 1) % 1000) + 1;
    case "epoch":
      return date.toSeconds();
  }
  return null;
}

createFunction("interval", [String], Duration, parseDuration, true);

createFunction(
  "date_bin",
  [Duration, DateTime, DateTime],
  DateTime,
  dateBin,
  true,
);
createFunction(
  "date_bin",
  [String, DateTime, DateTime],
  DateTime,
  dateBin,
  true,
);
function dateBin(
  stride: Duration | string,
  source: DateTime,
  origin: DateTime,
) {
  if (typeof stride === "string") {
    return dateBin(parseDuration(stride), source, origin);
  }
  const elapsed = source.diff(origin).toMillis();
  const intervalMs = stride.toMillis();
  if (intervalMs <= 0) {
    return null;
  }

  const binsElapsed = Math.floor(elapsed / intervalMs);
  return origin.plus(stride.toMillis() * binsElapsed);
}

createFunction("exists", [EvalQuery], Boolean, exists, true);
function exists(
  results: [{ name: symbol; type: DType }[], unknown[][]],
): boolean {
  const [_, data = []] = results || [];
  return data.length > 0;
}

// Aggregators
createAggregatorFunction("count", [ASTERISK], Number, count);
createAggregatorFunction("count", [Object], Number, count);
function count(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
  context: any,
) {
  if (state === "init") {
    store[self.handle] = [];
    return;
  } else if (state === "update") {
    const value = self.operands[0].resolve(context);
    if (self.operands[0].type !== ASTERISK && isNull(value)) {
      return;
    }
    store[self.handle].push(value);
    return;
  } else if (state === "finalize") {
    if (self.distinct) {
      store[self.handle] = new Set(store[self.handle]).size;
      return;
    }
    store[self.handle] = store[self.handle].length;
  }
}

createAggregatorFunction("sum", [INTEGER], INTEGER, sum);
createAggregatorFunction("sum", [Number], Number, sum);
createAggregatorFunction("sum", [Decimal], Decimal, sum);
function sum(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
  context: any,
) {
  const operand = self.operands[0];
  if (state === "init") {
    if (operand.type === Decimal) {
      store[self.handle] = new Decimal("0.00");
    } else {
      store[self.handle] = 0;
    }
  } else if (state === "update") {
    const value = operand.resolve(context);
    const type = typeOf(value);
    if (!isSameType(type, operand.type)) {
      throw new OperationalError(
        `Invalid type expected: sum(${typeName(operand.type)})  got: sum(${typeName(type)})`,
      );
    }
    if (!isNull(value)) {
      if (operand.type === Decimal) {
        store[self.handle] = (store[self.handle] as Decimal).add(value);
      } else {
        store[self.handle] += value;
      }
    }
  }
}

createAggregatorFunction("avg", [Number], Number, avg);
createAggregatorFunction("avg", [Decimal], Decimal, avg);
function avg(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
  context: any,
) {
  const operand = self.operands[0];
  if (state === "init") {
    store[self.handle] = [];
  } else if (state === "update") {
    const value = operand.resolve(context);
    const type = typeOf(value);
    if (!isNull(value)) {
      if (type !== operand.type) {
        throw new OperationalError(
          `Invalid type expected: avg(${typeName(operand.type)})  got: avg(${typeName(type)})`,
        );
      }
      store[self.handle].push(new Decimal(value));
    }
  } else if (state === "finalize") {
    const data = store[self.handle] as Decimal[];
    if (data.length) {
      const value: Decimal = data.reduce(
        (acc: Decimal, value: NumberSource) => {
          return acc.add(value);
        },
        Decimal.ZERO,
      );
      store[self.handle] = value.div(data.length);
    } else {
      store[self.handle] = Decimal.ZERO;
    }
  }
}

createAggregatorFunction("array_agg", [INTEGER], [INTEGER], arrayAgg);
createAggregatorFunction("array_agg", [Boolean], [Boolean], arrayAgg);
createAggregatorFunction("array_agg", [Number], [Number], arrayAgg);
createAggregatorFunction("array_agg", [Decimal], [Decimal], arrayAgg);
createAggregatorFunction("array_agg", [String], [String], arrayAgg);
function arrayAgg(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
  context: any,
) {
  if (state === "init") {
    store[self.handle] = [];
  } else if (state === "update") {
    const value = self.operands[0].resolve(context);
    if (!isNull(value)) {
      store[self.handle].push(value);
    }
  }
}

createAggregatorFunction("group_concat", [String, String], String, groupConcat);
function groupConcat(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
  context: any,
) {
  if (state === "init") {
    store[self.handle] = [];
  } else if (state === "update") {
    const value = self.operands[0].resolve(context);
    if (!isNull(value)) {
      store[self.handle].push(value);
    }
  } else if (state === "finalize") {
    const value = self.operands[1].resolve(context);
    store[self.handle] = store[self.handle].join(value);
  }
}

createAggregatorFunction("row_number", [], Number, rowNumber);
function rowNumber(self: EvalAggregator, store: any[], state: AggregatorState) {
  if (state === "init") {
    store[self.handle] = 1;
  } else if (state === "finalize") {
    if (self.windowState) {
      store[self.handle] = self.windowState.index + 1;
    }
  }
}

createAggregatorFunction("first_value", [Object], Object, firstValue);
function firstValue(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
) {
  if (state === "init") {
    store[self.handle] = null;
  } else if (state === "finalize") {
    if (self.windowState) {
      const value = self.operands[0].resolve(self.windowState.data[0]);
      store[self.handle] = value;
    }
  }
}

createAggregatorFunction("last_value", [Object], Object, lastValue);
function lastValue(self: EvalAggregator, store: any[], state: AggregatorState) {
  if (state === "init") {
    store[self.handle] = null;
  } else if (state === "finalize") {
    if (self.windowState) {
      const value = self.operands[0].resolve(
        self.windowState.data[self.windowState.data.length - 1],
      );
      store[self.handle] = value;
    }
  }
}

createAggregatorFunction("nth_value", [Object, Number], Object, nthValue);
function nthValue(self: EvalAggregator, store: any[], state: AggregatorState) {
  if (state === "init") {
    store[self.handle] = null;
  } else if (state === "finalize") {
    if (self.windowState) {
      const index = self.operands[1].resolve() as number;
      const value = self.operands[0].resolve(self.windowState.data[index - 1]);
      store[self.handle] = value;
    }
  }
}

createAggregatorFunction("lead", [Object], Object, lead);
createAggregatorFunction("lead", [Object, Number], Object, lead);
createAggregatorFunction("lead", [Object, Number, Object], Object, lead);
function lead(self: EvalAggregator, store: any[], state: AggregatorState) {
  if (state === "init") {
    store[self.handle] = null;
  } else if (state === "finalize") {
    if (self.windowState) {
      const offset = (self.operands[1]?.resolve() as number) ?? 1;
      const fallback = self.operands[2]?.resolve() ?? null;
      if (
        self.windowState.index + offset >=
        self.windowState.fullPartition.length
      ) {
        store[self.handle] = fallback;
      } else {
        const value =
          self.operands[0].resolve(
            self.windowState.fullPartition[self.windowState.index + offset],
          ) ?? fallback;
        store[self.handle] = value;
      }
    }
  }
}

createAggregatorFunction("lag", [Object], Object, lag);
createAggregatorFunction("lag", [Object, Number], Object, lag);
createAggregatorFunction("lag", [Object, Number, Object], Object, lag);
function lag(self: EvalAggregator, store: any[], state: AggregatorState) {
  if (state === "init") {
    store[self.handle] = null;
  } else if (state === "finalize") {
    if (self.windowState) {
      const offset = (self.operands[1]?.resolve() as number) ?? 1;
      const fallback = self.operands[2]?.resolve() ?? null;
      if (self.windowState.index - offset < 0) {
        store[self.handle] = fallback;
      } else {
        const value =
          self.operands[0].resolve(
            self.windowState.fullPartition[self.windowState.index - offset],
          ) ?? fallback;
        store[self.handle] = value;
      }
    }
  }
}

createAggregatorFunction("rank", [], Number, rank);
function rank(self: EvalAggregator, store: any[], state: AggregatorState) {
  if (state === "init") {
    store[self.handle] = 1;
  } else if (state === "finalize") {
    if (self.windowState) {
      let rank = 1;
      const cache = new Map<unknown, number>();
      const rankMap = new Map<unknown, number>();
      for (let i = 0; i <= self.windowState.index; i++) {
        const row = self.windowState.fullPartition[i];
        const a = self.windowState.orderValue(row);
        const id = [...cache.keys()].find((it) => isEqual(it, a));
        if (id) {
          rankMap.set(row, cache.get(id));
          rank++;
        } else {
          cache.set(a, rank);
          rankMap.set(row, rank);
          rank++;
        }
      }
      store[self.handle] = rankMap.get(
        self.windowState.fullPartition[self.windowState.index],
      );
    }
  }
}

createAggregatorFunction("dense_rank", [], Number, denseRank);
function denseRank(self: EvalAggregator, store: any[], state: AggregatorState) {
  if (state === "init") {
    store[self.handle] = 1;
  } else if (state === "finalize") {
    if (self.windowState) {
      let rank = 1;
      const cache = new Map<unknown, number>();
      const rankMap = new Map<unknown, number>();
      for (let i = 0; i <= self.windowState.index; i++) {
        const row = self.windowState.fullPartition[i];
        const a = self.windowState.orderValue(row);
        const id = [...cache.keys()].find((it) => isEqual(it, a));
        if (id) {
          rankMap.set(row, cache.get(id));
        } else {
          cache.set(a, rank);
          rankMap.set(row, rank);
          rank++;
        }
      }
      store[self.handle] = rankMap.get(
        self.windowState.fullPartition[self.windowState.index],
      );
    }
  }
}

createAggregatorFunction("first", [Object], Object, first);
function first(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
  context: any,
) {
  if (state === "init") {
    store[self.handle] = null;
  } else if (state === "update") {
    const value = self.operands[0].resolve(context);
    if (isNull(store[self.handle])) {
      store[self.handle] = value;
    }
  }
}

createAggregatorFunction("last", [Object], Object, last);
function last(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
  context: any,
) {
  if (state === "init") {
    store[self.handle] = null;
  } else if (state === "update") {
    const value = self.operands[0].resolve(context);
    store[self.handle] = value;
  }
}

createAggregatorFunction("min", [Object], Object, min);
function min(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
  context: any,
) {
  if (state === "init") {
    store[self.handle] = null;
  } else if (state === "update") {
    const value = self.operands[0].resolve(context);
    if (!isNull(value)) {
      const current = store[self.handle];
      if (isNull(current) || lt(value, current) || value < current) {
        store[self.handle] = value;
      }
    }
  }
}

createAggregatorFunction("max", [Object], Object, max);
function max(
  self: EvalAggregator,
  store: any[],
  state: AggregatorState,
  context: any,
) {
  if (state === "init") {
    store[self.handle] = null;
  } else if (state === "update") {
    const value = self.operands[0].resolve(context);
    if (!isNull(value)) {
      const current = store[self.handle];
      if (isNull(current) || gt(value, current) || value > current) {
        store[self.handle] = value;
      }
    }
  }
}
