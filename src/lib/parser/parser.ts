import { CompilationError, ParseError, ProgrammingError } from "../errors";
import {
  AllExpression,
  AnyExpression,
  AsteriskExpression,
  AttributeExpression,
  BooleanExpression,
  ColumnExpression,
  Expression,
  FromClause,
  FunctionExpression,
  GroupByExpression,
  ListExpression,
  LiteralExpression,
  OrderByExpression,
  OrderExpression,
  PivotByExpression,
  PlaceHolderExpression,
  Query,
  SelectClause,
  SubscriptExpression,
  SubQueryExpression,
  TableExpression,
  TargetExpression,
  QueryExpression,
  Op,
  InsertExpression,
  CreateTableExpression,
  WildcardExpression,
  OptionExpression,
  OverExpression,
  JoinExpression,
  CaseExpression,
  CollectionExpression,
  CastExpression,
  StatementExpression,
  Constraint,
  PrimaryKeyConstraint,
  UniqueConstraint,
  ForeignKeyConstraint,
  CheckConstraint,
  ColumnDefinition,
} from "./ast";
import { parser } from "./bql.grammar";
import { SyntaxNode, Tree, type SyntaxNodeRef } from "@lezer/common";
import { DateTime, Duration, DurationLikeObject } from "luxon";

export function readString(value: string) {
  if (value.startsWith('"') && value.endsWith('"')) {
    return value.slice(1, -1);
  } else if (value.startsWith("'") && value.endsWith("'")) {
    return value.slice(1, -1).replace(/''/g, "'");
  }
  return value;
}

const binaryNodes = [
  "Lt",
  "Lte",
  "Gt",
  "Gte",
  "Eq",
  "Neq",
  "In",
  "NotIn",
  "Match",
  "NotMatch",
  "Matches",
  "ExprAnd",
  "ExprOr",
  "CalcExpr",
  "IsTrue",
  "IsNotTrue",
];
const unaryNodes = ["ExprNot", "IsNull", "IsNotNull"];

// Regex patterns
const INTERVAL_REGEX =
  /([-+]?\s*\d+\s*\.?\s*\d*)\s*(years|year|y|quarters|quarter|q|months|month|mons|mon|mo|weeks|week|wk|days|day|d|hours|hour|h|minutes|minute|mins|min|m|seconds|second|secs|sec|s|milliseconds|millisecond|msecs|msec|ms|microseconds|microsecond|µs|us)/gi;
const TIME_REGEX = /([-+]?)\s*(\d{1,2}:\d{1,2}(:\d{1,2})?(\.\d+)?)/g; // HH:MM:SS.mmm format
const ISO_8601_REGEX =
  /^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/;
const INTERVAL_VALIDATION_REGEX =
  /^\s*((\s*[-+]?\s*\d+\s*\.?\s*\d*)\s*(years|year|y|quarters|quarter|q|months|month|mons|mon|mo|weeks|week|wk|days|day|d|hours|hour|h|minutes|minute|mins|min|m|seconds|second|secs|sec|s|milliseconds|millisecond|msecs|msec|ms|microseconds|microsecond|µs|us)|(\s*([-+]?)\s*(\d{1,2}:\d{1,2}(:\d{1,2})?(\.\d+)?)))+\s*$/;
/**
 * Validates a PostgreSQL interval string, including SQL standard and ISO 8601
 * formats.
 *
 * @param intervalStr The interval string to validate.
 * @returns True if the input is valid, false otherwise.
 */
export function isValidInterval(intervalStr: string): {
  valid: boolean;
  type?: "iso" | "sql";
} {
  const cleanedStr = intervalStr.trim().replace(/^@?\s*/, "");

  // Check for ISO 8601 format
  if (ISO_8601_REGEX.test(cleanedStr)) return { valid: true, type: "iso" };

  // Check for SQL standard and colon-separated time formats
  return { valid: INTERVAL_VALIDATION_REGEX.test(cleanedStr), type: "sql" };
}

/**
 * Parses a PostgreSQL interval string, supporting SQL standard and ISO 8601
 * formats.
 *
 * @param intervalStr The interval string to parse.
 * @returns An object containing the parsed interval values.
 */
export function parseDuration(intervalStr: string): Duration {
  const validation = isValidInterval(intervalStr);
  if (!validation.valid) {
    throw new ProgrammingError(
      `invalid input syntax for type interval: "${intervalStr}"`,
    );
  }

  const result: DurationLikeObject = {};

  if (validation.type === "iso") {
    return Duration.fromISO(intervalStr);
  }

  // Match standard SQL and abbreviated units
  let match;
  while ((match = INTERVAL_REGEX.exec(intervalStr)) !== null) {
    const value = parseFloat(match[1].replaceAll(/\s+/g, ""));
    const unit = match[2].toLowerCase().trim();

    switch (unit) {
      case "y":
      case "year":
      case "years":
        result.years = (result.years || 0) + value;
        break;
      case "q":
      case "quarter":
      case "quarters":
        result.quarters = (result.quarters || 0) + value;
        break;
      case "mo":
      case "mon":
      case "mons":
      case "month":
      case "months":
        result.months = (result.months || 0) + value;
        break;
      case "wk":
      case "week":
      case "weeks":
        result.weeks = (result.weeks || 0) + value;
        break;
      case "d":
      case "day":
      case "days":
        result.days = (result.days || 0) + value;
        break;
      case "h":
      case "hour":
      case "hours":
        result.hours = (result.hours || 0) + value;
        break;
      case "m":
      case "min":
      case "mins":
      case "minute":
      case "minutes":
        result.minutes = (result.minutes || 0) + value;
        break;
      case "s":
      case "sec":
      case "secs":
      case "second":
      case "seconds":
        result.seconds = (result.seconds || 0) + value;
        break;
      case "ms":
      case "msec":
      case "msecs":
      case "millisecond":
      case "milliseconds":
        result.milliseconds = (result.milliseconds || 0) + value;
        break;
      case "µs":
      case "us":
      case "microsecond":
      case "microseconds":
        result.milliseconds = (result.milliseconds || 0) + value / 1000;
        break;
    }
  }

  // Match colon-separated time formats (HH:MM:SS, HH:MM:SS.mmm)
  while ((match = TIME_REGEX.exec(intervalStr)) !== null) {
    const [_, dir, time] = match;
    const factor = dir === "-" ? -1 : 1;
    const parts = time.split(":").map((it) => parseFloat(it) * factor);
    const [hours, minutes, seconds = null, ms = null] = parts;

    result.hours = (result.hours || 0) + hours;
    result.minutes = (result.minutes || 0) + minutes;
    if (seconds !== null) {
      result.seconds = (result.seconds || 0) + seconds;
    }

    if (ms !== null) {
      result.milliseconds = (result.milliseconds || 0) + ms;
    }
  }

  return Duration.fromObject(result);
}

export function parseQuery(
  query: string,
): [Expression, ParseError[], OptionExpression[]] {
  const parser = new Parser(query);
  return [parser.query, parser.errors, parser.options];
}

export function parseDurationx(interval: string): Duration {
  const regex =
    /(?:(-?\s*\d+)\s*years?)?\s*(?:(-?\s*\d+)\s*quarters?)?\s*(?:(-?\s*\d+)\s*mont?h?s?)?\s*(?:(-?\s*\d+)\s*weeks?)?\s*(?:(-?\s*\d+)\s*days?)?\s*(?:(-?\s*\d+):(\d+):(\d+(?:\.\d+)?))?/;
  const match = interval.match(regex);

  if (!match) {
    return Duration.fromISO(interval);
  }

  const [, years, quarters, months, weeks, days, hours, minutes, seconds] =
    match.map((v) =>
      v === undefined ? 0 : parseFloat(v.replaceAll(/\s+/g, "")),
    );

  const duration: Record<string, number> = {
    years,
    quarters,
    months,
    weeks,
    days,
    hours,
    minutes,
    seconds,
  };

  Object.keys(duration).forEach((key) => {
    if (!duration[key]) {
      delete duration[key];
    }
  });

  return Duration.fromObject(duration);
}

export class Parser {
  tree: Tree;

  readonly errors: Array<ParseError> = [];
  readonly stack: Array<Expression> = [];
  readonly options: Array<OptionExpression> = [];

  constructor(readonly qs: string) {
    this.tree = parser.configure({ strict: false }).parse(qs);
    this.tree.cursor().iterate(
      (node) => {
        try {
          return this.enter(node);
        } catch (e: unknown) {
          this.errors.push(this.error(node, e.toString(), e));
        }
      },
      (node) => {
        try {
          return this.leave(node);
        } catch (e: unknown) {
          this.errors.push(this.error(node, e.toString(), e));
        }
      },
    );
  }

  protected error(
    node: SyntaxNodeRef | SyntaxNode,
    message: string,
    cause?: unknown,
  ): ParseError {
    return new ParseError(message, {
      node: node.toString(),
      position: { from: node.from, to: node.to },
      content: this.content(node),
      cause,
    });
  }

  private isRootExpression(expr: Expression) {
    return (
      expr instanceof Query ||
      expr instanceof CreateTableExpression ||
      expr instanceof InsertExpression
    );
  }

  #query: Expression;
  get query(): Expression {
    if (this.#query) return this.#query;
    if (this.stack.length !== 1) {
      if (this.stack.length > 1 && this.stack.every(this.isRootExpression)) {
        this.#query = new StatementExpression(this.stack);
        return this.#query;
      }
      throw new CompilationError(
        `Invalid query stack(${this.stack.length})`,
        this.stack[this.stack.length - 1],
      );
    }
    this.#query = this.stack.pop();
    return this.#query;
  }

  get top() {
    return this.stack[this.stack.length - 1];
  }

  get lastQuery(): Query {
    const qs = this.top;
    if (qs instanceof Query) {
      return qs;
    }
    throw new CompilationError(
      `Unexpected node. Expected Query found(${qs.constructor.name}): ${qs}`,
      qs,
    );
  }

  protected cast(node: SyntaxNode, expression: Expression) {
    if (node) {
      const args: Expression[] = [expression];
      const timezone = node.getChild("Timezone");
      if (timezone) {
        args.push(
          new LiteralExpression(
            { pos: timezone.from },
            readString(this.content(timezone.getChild("Zone"))),
          ),
        );
        return new FunctionExpression(
          expression.parseInfo,
          readString(this.content(node.getChild("Type"))),
          args,
        );
      }

      return new CastExpression(
        expression.parseInfo,
        readString(this.content(node.getChild("Type"))),
        args[0],
      );
    }
    return expression;
  }

  protected enter(node: SyntaxNodeRef) {
    const parseInfo = this.parseInfo(node);
    if (node.type.isError) {
      const identifier = this.content(node);
      if (identifier.match(/^[a-zA-Z_][a-zA-Z0-9_]*$/)) {
        if (node.node.parent.name === "As") {
          return;
        }

        // if(identifier.toUpperCase() === "ALL") {
        //   this.add(
        //     new FunctionExpression(
        //       parseInfo,
        //       "all",
        //       [],
        //       node.node.getChild("Distinct") ? true : false,
        //     ),
        //   );
        //   return;
        // }
      }
      this.errors.push(
        this.error(
          node.node.parent,
          `syntax error at or near: "${this.content(node)}"`,
        ),
      );
    } else if (node.name === "ExprCase") {
      this.add(new CaseExpression(parseInfo, []));
    } else if (node.name === "Insert") {
      const columns = (
        node.node.getChild("Columns")?.getChildren("Identifier") ?? []
      ).map((it) => this.content(it));

      this.add(
        new InsertExpression(
          parseInfo,
          this.content(node.node.getChild("Identifier")),
          columns,
          [],
        ),
      );
    } else if (node.name == "Statement") {
      this.stack.push(new Query(new SelectClause(parseInfo, [])));
    } else if (node.name === "Select") {
      this.lastQuery.select = new SelectClause(
        parseInfo,
        [],
        node.node.getChild("Distinct") ? true : false,
      );
    } else if (node.name === "Target") {
      if (node.node.getChild("ASTERISK")) {
        this.add(new AsteriskExpression(parseInfo));
      }
    } else if (node.name === "Gt") {
      this.add(new BooleanExpression(parseInfo, ">", []));
    } else if (node.name === "Lt") {
      this.add(new BooleanExpression(parseInfo, "<", []));
    } else if (node.name === "Gte") {
      this.add(new BooleanExpression(parseInfo, ">=", []));
    } else if (node.name === "Lte") {
      this.add(new BooleanExpression(parseInfo, "<=", []));
    } else if (node.name === "Eq") {
      this.add(new BooleanExpression(parseInfo, "=", []));
    } else if (node.name === "Neq") {
      this.add(new BooleanExpression(parseInfo, "!=", []));
    } else if (node.name === "In") {
      this.add(new BooleanExpression(parseInfo, "IN", []));
    } else if (node.name === "NotIn") {
      this.add(new BooleanExpression(parseInfo, "NOTIN", []));
    } else if (node.name === "Match") {
      this.add(
        new BooleanExpression(
          parseInfo,
          this.content(node.node.getChild("Op")) as Op,
          [],
        ),
      );
    } else if (node.name === "NotMatch") {
      this.add(
        new BooleanExpression(
          parseInfo,
          this.content(node.node.getChild("Op")) as Op,
          [],
        ),
      );
    } else if (node.name === "Matches") {
      this.add(
        new BooleanExpression(
          parseInfo,
          this.content(node.node.getChild("Op")) as Op,
          [],
        ),
      );
    } else if (node.name === "IsNull") {
      this.add(new BooleanExpression(parseInfo, "ISNULL", []));
    } else if (node.name === "IsNotNull") {
      this.add(new BooleanExpression(parseInfo, "ISNOTNULL", []));
    } else if (node.name === "IsTrue") {
      this.add(new BooleanExpression(parseInfo, "=", []));
    } else if (node.name === "IsNotTrue") {
      this.add(new BooleanExpression(parseInfo, "!=", []));
    } else if (node.name === "Between") {
      this.add(new BooleanExpression(parseInfo, "BETWEEN", []));
    } else if (node.name === "NotBetween") {
      this.add(new BooleanExpression(parseInfo, "NOTBETWEEN", []));
    } else if (node.name === "ExprAnd") {
      this.add(new BooleanExpression(parseInfo, "AND", []));
    } else if (node.name === "ExprOr") {
      this.add(new BooleanExpression(parseInfo, "OR", []));
    } else if (node.name === "ExprNot") {
      this.add(new BooleanExpression(parseInfo, "NOT", []));
    } else if (node.name === "Column") {
      this.add(
        this.cast(
          node.node.getChild("Cast"),
          new ColumnExpression(
            parseInfo,
            this.content(node.node.getChild("Identifier")),
          ),
        ),
      );
    } else if (node.name === "Wildcard") {
      this.add(
        new WildcardExpression(
          parseInfo,
          this.content(node.node.getChild("Identifier")),
        ),
      );
    } else if (node.name === "Placeholder") {
      this.add(
        new PlaceHolderExpression(
          parseInfo,
          this.content(node.node.getChild("Identifier")),
        ),
      );
    } else if (node.name === "Literal") {
      this.add(
        this.cast(
          node.node.getChild("Cast"),
          this.literal(node.node.firstChild),
        ),
      );
    } else if (node.name === "List") {
      this.add(
        new ListExpression(
          parseInfo,
          node.node.getChildren("Literal").map((it) => this.literal(it)),
        ),
      );
      return false;
    } else if (node.name === "Function") {
      this.add(
        new FunctionExpression(
          parseInfo,
          this.content(node.node.getChild("Identifier")),
          [],
          node.node.getChild("Distinct") ? true : false,
        ),
      );
    } else if (node.name === "GroupBy") {
      this.add(new GroupByExpression(parseInfo, []));
    } else if (node.name === "OrderBy") {
      this.add(new OrderByExpression(parseInfo, []));
    } else if (node.name === "CalcExpr") {
      if (node.node.getChild("PLUS")) {
        this.add(new BooleanExpression(parseInfo, "+", []));
      } else if (node.node.getChild("MINUS")) {
        this.add(new BooleanExpression(parseInfo, "-", []));
      } else if (node.node.getChild("ASTERISK")) {
        this.add(new BooleanExpression(parseInfo, "*", []));
      } else if (node.node.getChild("DIV")) {
        this.add(new BooleanExpression(parseInfo, "/", []));
      } else if (node.node.getChild("MOD")) {
        this.add(new BooleanExpression(parseInfo, "%", []));
      }
    }
  }

  protected literal(node: SyntaxNodeRef): LiteralExpression {
    const parseInfo = this.parseInfo(node);
    if (node.name === "Literal") {
      return this.literal(node.node.firstChild);
    }
    if (["Decimal", "Integer"].includes(node.name)) {
      return new LiteralExpression(parseInfo, +this.content(node));
    } else if (node.name === "Boolean") {
      return new LiteralExpression(
        parseInfo,
        this.content(node).toLowerCase() === "true",
      );
    } else if (node.name === "Null") {
      return new LiteralExpression(parseInfo, null);
    } else if (node.name === "String") {
      return new LiteralExpression(parseInfo, readString(this.content(node)));
    } else if (node.name === "Date") {
      const value = this.content(node);
      const date = DateTime.fromFormat(value, "yyyy-MM-dd");
      if (!date.isValid) {
        throw new ProgrammingError(
          `invalid input syntax for type date: "${value}"`,
        );
      }
      return new LiteralExpression(parseInfo, date);
    } else if (node.name === "Interval") {
      const value = readString(this.content(node.node.getChild("String")));
      const duration = parseDuration(value);
      if (!duration.isValid) {
        throw new ProgrammingError(
          `invalid input syntax for type interval: "${value}"`,
        );
      }
      return new LiteralExpression(parseInfo, duration);
    } else if (node.name === "Timestamp") {
      const value = readString(this.content(node.node.getChild("String")));
      const timestamp = [DateTime.fromSQL(value), DateTime.fromISO(value)].find(
        (it) => it.isValid,
      );
      if (!timestamp) {
        throw new ProgrammingError(
          `invalid input syntax for type timestamp: "${value}"`,
        );
      }
      return new LiteralExpression(parseInfo, timestamp);
    } else if (node.name === "TimestampWithTimeZone") {
      const value = readString(this.content(node.node.getChild("String")));
      const timestamp = [
        DateTime.fromSQL(value, { setZone: true }),
        DateTime.fromISO(value, { setZone: true }),
      ].find((it) => it.isValid);
      if (!timestamp) {
        throw new ProgrammingError(
          `invalid input syntax for type timestamp: "${value}"`,
        );
      }
      return new LiteralExpression(parseInfo, timestamp);
    } else if (node.name === "TimestamptzAtTimeZone") {
      const value = readString(this.content(node.node.getChild("String")));
      const zone = readString(this.content(node.node.getChild("Zone")));
      const timestamp = [
        DateTime.fromSQL(value, { zone }),
        DateTime.fromISO(value, { zone }),
      ].find((it) => it.isValid);
      if (!timestamp) {
        throw new ProgrammingError(
          `invalid input syntax for type timestamp: "${value}"`,
        );
      }
      return new LiteralExpression(parseInfo, timestamp);
    }
    throw new Error(`Unsupported Literal node: ${node}`);
  }

  protected parseInfo(node: SyntaxNodeRef) {
    return { pos: node.from };
  }

  protected leave(node: SyntaxNodeRef) {
    const parseInfo = this.parseInfo(node);
    if (node.name === "Target") {
      if (this.stack.length) {
        const expr = this.stack.pop();
        const query = this.lastQuery;
        const alias = this.getAlias(node);
        query.select.targets.push(new TargetExpression(parseInfo, expr, alias));
      }
    } else if (node.name === "Union") {
      const expr = this.stack.pop();
      if (expr instanceof Query) {
        this.lastQuery.unions.push({
          type: this.content(node.node.getChild("Type")).toUpperCase(),
          query: expr,
        });
      } else {
        throw new CompilationError(
          `Expected query found(${expr.constructor.name}): ${expr}`,
          expr,
        );
      }
    } else if (node.name === "TypeCast") {
      this.stack[this.stack.length - 1] = this.cast(
        node.node.getChild("Cast"),
        this.stack[this.stack.length - 1],
      );
    } else if (node.name === "Else") {
      const expr = this.stack.pop();
      if (this.top instanceof CaseExpression) {
        this.top.fallback = expr;
      }
    } else if (node.name === "When") {
      const [then, when] = [this.stack.pop(), this.stack.pop()];
      if (this.top instanceof CaseExpression) {
        this.top.conditions.push({ when, then });
      }
    } else if (node.name === "Initial") {
      const expr = this.stack.pop();
      if (this.top instanceof CaseExpression) {
        this.top.value = expr;
      }
    } else if (node.name === "ExprCase") {
      const top = this.top;
      if (top instanceof CaseExpression && top.value) {
        top.conditions.forEach((expr, i) => {
          top.conditions[i].when = new BooleanExpression(top.parseInfo, "=", [
            top.value,
            expr.when,
          ]);
        });
      }
    } else if (node.name === "Option") {
      const expr = this.stack.pop();
      if (expr instanceof LiteralExpression || expr instanceof ListExpression) {
        this.options.push(
          new OptionExpression(
            parseInfo,
            readString(
              this.content(
                node.node.getChild("Identifier") ??
                  node.node.getChild("String") ??
                  node.node.getChild("Keyword"),
              ) ||
                this.content(node)
                  .replaceAll(/^SET\s*([^=]+).*/gi, "$1")
                  .trim(),
            ),
            expr,
          ),
        );
      }
    } else if (node.name === "Values") {
      const expressions = Array.from({
        length: node.node.getChildren("Value").length,
      })
        .map(() => this.stack.pop())
        .reverse();
      if (this.top instanceof InsertExpression) {
        this.top.values.push(expressions);
      }
    } else if (node.name === "CreateTable") {
      const tableName = this.content(node.node.getChild("Identifier"));
      const columns: ColumnDefinition[] = [];
      const constraints: Constraint[] = [];
      let child = node.node.getChild("Columns").lastChild;
      while (child) {
        if (child.name === "ColumnType") {
          const type = this.content(child.getChild("Type"));
          const options = this.content(child.getChild("Options"))
            .trim()
            .replaceAll(/\s+/g, " ")
            .toUpperCase();
          columns.push(
            new ColumnDefinition(
              this.content(child.getChild("Identifier")),
              type,
              child.getChild("Array") ? true : false,
              options.includes("PRIMARY KEY"),
              options.includes("NOT NULL"),
              child.getChild("Check") ? this.stack.pop() : undefined,
            ),
          );
        } else if (child.name === "Constraint") {
          let type = child.firstChild;
          let name = "";
          if (type.name === "Identifier") {
            name = this.content(type);
            type = type.nextSibling;
          }
          if (type.name === "PrimaryKey") {
            constraints.push(
              new PrimaryKeyConstraint(
                tableName,
                type.getChildren("Identifier").map((it) => this.content(it)),
                name,
              ),
            );
          } else if (type.name === "Unique") {
            constraints.push(
              new UniqueConstraint(
                tableName,
                type.getChildren("Identifier").map((it) => this.content(it)),
                name,
              ),
            );
          } else if (type.name === "ForeignKey") {
            constraints.push(
              new ForeignKeyConstraint(
                tableName,
                (
                  type.getChild("SourceColumns")?.getChildren("Identifier") ??
                  []
                ).map((it) => this.content(it)),
                (
                  type.getChild("TargetColumns")?.getChildren("Identifier") ??
                  []
                ).map((it) => this.content(it)),
                this.content(type.getChild("TargetTable")),
                name,
              ),
            );
          } else if (type.name === "Check") {
            constraints.push(
              new CheckConstraint(tableName, this.stack.pop(), name),
            );
          }
        }
        child = child.prevSibling;
      }

      const query: Query = node.node.getChild("As")
        ? (this.stack.pop() as Query)
        : undefined;
      const expr = new CreateTableExpression(
        parseInfo,
        tableName,
        node.node.getChild("NotExists") ? true : false,
        columns.reverse(),
        this.content(node.node.getChild("Using")),
        constraints.reverse(),
        query,
      );
      this.stack.push(expr);
    } else if (node.name === "Over" || node.name === "Window") {
      let expr = new OverExpression(parseInfo).copy({
        name: this.content(node.node.getChild("Identifier")),
      });
      const range = node.node.getChild("Frame")?.getChild("Range");
      const orderBy = node.node.getChild("OrderBy");
      const frame: {
        type: string;
        preceding: number;
        following: number;
        exclude: string;
      } = {
        type: this.content(node.node.getChild("Frame")?.getChild("Type")) || "",
        preceding: Infinity,
        following: Infinity,
        exclude: (
          node.node.getChild("Exclude")?.firstChild?.name || "NONE"
        ).toUpperCase(),
      };

      if (orderBy) {
        if (!frame.type) {
          frame.type = "RANGE";
          frame.preceding = Infinity;
          frame.following = 0;
        }
      } else if (!frame.type) {
        frame.type = "ROWS";
      }

      if (range) {
        if (range.getChild("Lower")?.getChild("Unbounded")) {
          frame.preceding = Infinity;
        } else if (range.getChild("Lower")?.getChild("Integer")) {
          frame.preceding = +this.content(
            range.getChild("Lower")?.getChild("Integer"),
          );
        } else if (range.getChild("Lower")?.getChild("Current")) {
          frame.preceding = 0;
        }

        if (range.getChild("Upper")?.getChild("Unbounded")) {
          frame.following = Infinity;
        } else if (range.getChild("Upper")?.getChild("Integer")) {
          frame.following = +this.content(
            range.getChild("Lower")?.getChild("Integer"),
          );
        } else if (range.getChild("Upper")?.getChild("Current")) {
          frame.following = 0;
        }
      }

      if (range || !expr.name || node.node.parent?.name === "Windows") {
        expr = expr.copy({ frame });
      }

      if (orderBy) {
        const value = this.stack.pop();
        if (value instanceof OrderByExpression) {
          expr = expr.copy({ orderBy: value });
        } else {
          throw new CompilationError(
            `Expected OrderByExpression found ${expr}`,
            value,
          );
        }
      }

      if (node.node.getChild("Partition")) {
        expr = expr.copy({ partitionBy: this.stack.pop() });
      }
      this.add(expr);
    } else if (node.name === "Function") {
      const exprs: Expression[] = [];
      const over = node.node.getChild("Over") ? this.stack.pop() : undefined;
      if (over) {
        if (!(over instanceof OverExpression)) {
          throw new CompilationError(
            `Expected OverExpression found: ${over}`,
            over,
          );
        }
      }
      const expr = node.node.getChild("ASTERISK");
      const filter = node.node.getChild("Filter")
        ? this.stack.pop()
        : undefined;
      if (expr) {
        const top = this.stack[this.stack.length - 1];
        if (top instanceof FunctionExpression) {
          top.args.push(new AsteriskExpression(this.parseInfo(expr)));
          top.window = over as OverExpression;
          top.filter = filter;
          this.stack[this.stack.length - 1] = this.cast(
            node.node.getChild("Cast"),
            top,
          );
        }
        return;
      }
      let n =
        node.node.getChild("Distinct") ??
        node.node.firstChild ??
        node.node.nextSibling;
      while (
        n?.nextSibling &&
        !["Filter", "Over", "Cast"].includes(n.nextSibling.name)
      ) {
        exprs.push(this.stack.pop());
        n = n.nextSibling;
      }
      const top = this.stack[this.stack.length - 1];
      if (top instanceof FunctionExpression) {
        top.filter = filter;
        top.window = over as OverExpression;
        top.args.push(...exprs.reverse());
        this.stack[this.stack.length - 1] = this.cast(
          node.node.getChild("Cast"),
          top,
        );
      }
    } else if (node.name === "ListExpr") {
      const exprs: Expression[] = [];

      let n = node.node.firstChild;
      while (n) {
        exprs.push(this.stack.pop());
        n = n.nextSibling;
      }
      this.add(new CollectionExpression(parseInfo, exprs.reverse()));
    } else if (node.name === "Windows") {
      const windows: Record<string, OverExpression> = {};
      node.node.getChildren("Window").forEach(() => {
        const expr = this.stack.pop();
        if (expr instanceof OverExpression) {
          if (windows[expr.name]) {
            throw new CompilationError(
              `Duplicate window expression: ${expr}, window ${expr.name} already exists as ${windows[expr.name]}`,
              expr,
            );
          }
          windows[expr.name] = expr;
        } else {
          throw new CompilationError(
            `Invalid window declaration, expected :OverExpression found: ${expr}`,
            expr,
          );
        }
      });
      this.lastQuery.windows = windows;
    } else if (node.name === "Groups") {
      const exprs: Expression[] = [];
      let n = node.node.firstChild;
      while (n) {
        const expr = this.stack.pop();
        exprs.push(expr);
        n = n.nextSibling;
      }
      const top = this.stack[this.stack.length - 1];
      if (top instanceof GroupByExpression) {
        top.columns.push(...exprs);
      }
    } else if (node.name === "Having") {
      const expr = this.stack.pop();
      const top = this.stack[this.stack.length - 1];
      if (top instanceof GroupByExpression && expr) {
        top.having = expr;
      }
    } else if (node.name === "GroupBy") {
      const expr = this.stack.pop();
      if (expr instanceof GroupByExpression) {
        this.lastQuery.groupBy = expr;
      }
    } else if (node.name === "Order") {
      if (this.stack.length) {
        const expr = this.stack.pop();
        this.stack.push(
          new OrderExpression(
            expr.parseInfo,
            expr,
            this.content(node.node.getChild("Direction")).toUpperCase() ===
            "DESC"
              ? "DESC"
              : "ASC",
          ),
        );
      }
    } else if (node.name === "OrderBy") {
      const exprs: OrderExpression[] = [];
      let n = node.node.firstChild;
      while (n) {
        const expr = this.stack.pop();
        if (expr instanceof OrderExpression) {
          exprs.push(expr);
        }
        n = n.nextSibling;
      }
      const top = this.stack[this.stack.length - 1];
      if (top instanceof OrderByExpression) {
        top.columns.push(...exprs.reverse());
        if (node.node.parent.name === "Statement") {
          this.stack.pop();
          this.lastQuery.orderBy = top;
        }
      }
    } else if (node.name === "Between" || node.name === "NotBetween") {
      const exprs = Array.from({ length: 3 }, () => this.stack.pop()).reverse();
      const top = this.stack[this.stack.length - 1];
      if (top instanceof BooleanExpression) {
        top.args.push(...exprs);
      }
    } else if (node.name === "Where") {
      const last = this.stack.pop();
      this.lastQuery.where = last;
    } else if (node.name === "SubSelect") {
      const subQuery = this.stack.pop();
      if (subQuery instanceof Query) {
        const alias = this.getAlias(node);
        this.add(
          alias
            ? new TargetExpression(
                parseInfo,
                new SubQueryExpression(parseInfo, subQuery),
                alias,
              )
            : new SubQueryExpression(parseInfo, subQuery),
        );
      }
    } else if (node.name === "WithExpr") {
      const subQuery = this.stack.pop();
      const name = this.content(node.node.getChild("Identifier"));
      if (!name) {
        throw new CompilationError(
          `Invalid WITH query expected a CTE name: ${subQuery}`,
          subQuery,
        );
      }
      if (subQuery instanceof Query) {
        if (this.lastQuery.commonTableExpressions[name]) {
          throw new CompilationError(
            `WITH query name "${name}" specified more than once`,
            subQuery,
          );
        }
        this.lastQuery.commonTableExpressions[name] = subQuery;
      }
    } else if (node.name === "From") {
      const last = this.stack.pop();
      this.lastQuery.from = new FromClause(parseInfo, last);
    } else if (node.name === "Relation") {
      const content = this.content(node).trim();
      if (content === "#") {
        this.add(new TableExpression(parseInfo, "", this.getAlias(node)));
      } else {
        const expr = this.stack.pop();
        if (expr instanceof ColumnExpression) {
          this.add(
            new TableExpression(
              expr.parseInfo,
              expr.column,
              this.getAlias(node),
            ),
          );
        } else {
          this.add(expr);
        }
      }
    } else if (node.name === "Relations") {
      const relations = node.node
        .getChildren("Relation")
        .filter((it) => {
          return this.content(it).trim() !== "#";
        })
        .map(() => this.stack.pop())
        .reverse();
      if (relations.length) {
        const [main, ...rest] = relations;
        const lastQuery = this.lastQuery;
        this.add(main);
        rest.forEach((table) => {
          lastQuery.joins.push(
            new JoinExpression(
              parseInfo,
              "CROSS",
              table,
              new LiteralExpression(parseInfo, true),
            ),
          );
        });
      }
    } else if (node.name === "Query") {
      const expr = this.stack.pop();
      if (expr instanceof TableExpression) {
        this.add(expr);
      } else {
        this.add(new QueryExpression(parseInfo, expr, this.getAlias(node)));
      }
    } else if (node.name === "CastExpr") {
      const type = readString(this.content(node.node.getChild("Identifier")));
      const expr = this.stack.pop();
      this.add(new FunctionExpression(expr.parseInfo, type, [expr]));
    } else if (node.name === "Limit") {
      this.lastQuery.limit = +this.content(node.node.getChild("Integer"));
    } else if (binaryNodes.includes(node.name)) {
      const right = this.stack.pop();
      const left = this.stack.pop();
      const top = this.stack[this.stack.length - 1];
      if (top instanceof BooleanExpression) {
        if (right instanceof FunctionExpression) {
          if (right.name === "any" && right.args.length == 1) {
            this.stack[this.stack.length - 1] = new AnyExpression(
              top.parseInfo,
              top.op,
              left,
              right.args[0],
            );
            return;
          } else if (right.name === "all" && right.args.length == 1) {
            this.stack[this.stack.length - 1] = new AllExpression(
              top.parseInfo,
              top.op,
              left,
              right.args[0],
            );
            return;
          }
        }
        top.args.push(left, right);
      }
    } else if (node.name === "Attribute") {
      const last = node.node.lastChild;
      const fn =
        last.name === "Function"
          ? (this.stack.pop() as FunctionExpression)
          : this.content(last);
      this.add(new AttributeExpression(parseInfo, this.stack.pop(), fn));
    } else if (node.name === "Subscript") {
      this.add(
        new SubscriptExpression(
          parseInfo,
          this.stack.pop(),
          readString(this.content(node.node.getChild("String"))),
        ),
      );
    } else if (node.name === "Uplus") {
      this.add(new BooleanExpression(parseInfo, "+", [this.stack.pop()]));
    } else if (node.name === "Uminus") {
      this.add(new BooleanExpression(parseInfo, "-", [this.stack.pop()]));
    } else if (node.name === "PivotBy") {
      const expr = new PivotByExpression(parseInfo, []);
      let n = node.node.firstChild;
      const exprs: Expression[] = [];
      while (n) {
        if (n.name === "Column") {
          exprs.push(this.stack.pop());
        }
        n = n.nextSibling;
      }

      n = node.node.firstChild;
      while (n) {
        if (n.name === "Column") {
          expr.columns.push(exprs.pop());
        } else if (n.name === "Integer") {
          expr.columns.push(+this.content(n));
        }
        n = n.nextSibling;
      }
      this.lastQuery.pivotBy = expr;
    } else if (unaryNodes.includes(node.name)) {
      const expr = this.stack.pop();
      const top = this.stack[this.stack.length - 1];
      if (top instanceof BooleanExpression) {
        top.args.push(expr);
      }
    } else if (node.name === "Join") {
      const parseInfo = this.parseInfo(node);
      let condition: Expression | null = node.node.getChild("On")
        ? this.stack.pop()
        : null;
      const source = node.node.getChild("Source");
      const name = source?.getChild("Identifier");
      const alias = this.getAlias(source);

      const table = name
        ? alias
          ? new TargetExpression(
              parseInfo,
              new TableExpression(parseInfo, this.content(name)),
              alias,
            )
          : new TableExpression(parseInfo, this.content(name))
        : this.stack.pop();
      const type =
        this.content(node.node.getChild("Type"))
          .toUpperCase()
          .replace("OUTER", "")
          .trim() || "INNER";

      let using = node.node.getChild("JoinUsing")?.firstChild;
      const columns: string[] = [];
      while (using) {
        columns.push(readString(this.content(using)));
        using = using.nextSibling;
      }

      const from = this.lastQuery.from;
      if (columns.length) {
        const names: string[] = [];
        if (from.from instanceof TableExpression) {
          names.push(from.from.table);
        } else if (from.from instanceof TargetExpression) {
          names.push(from.from.as);
        } else {
          throw new CompilationError(
            `FROM expression needs to have an alias: ${from.from}`,
            from.from,
          );
        }

        if (table instanceof TableExpression) {
          names.push(table.table);
        } else if (table instanceof TargetExpression) {
          names.push(table.as);
        } else {
          throw new CompilationError(
            `JOIN expression needs to have an alias: ${table}`,
            table,
          );
        }

        const [left, right] = names;
        let expr = new BooleanExpression(parseInfo, "AND", []);
        columns.forEach((column) => {
          expr.args.push(
            new BooleanExpression(parseInfo, "=", [
              new AttributeExpression(
                parseInfo,
                new ColumnExpression(parseInfo, left),
                column,
              ),
              new AttributeExpression(
                parseInfo,
                new ColumnExpression(parseInfo, right),
                column,
              ),
            ]),
          );

          if (expr.args.length == 2) {
            expr = new BooleanExpression(parseInfo, "AND", [expr]);
          }
        });

        condition = expr.args.length == 1 ? expr.args[0] : expr;
      }

      if (
        source.getChild("SubSelect") &&
        !(table instanceof TargetExpression)
      ) {
        throw new CompilationError(
          `Subqery expression missing alias: ${table}`,
          table,
        );
      }

      condition = condition ?? new LiteralExpression(parseInfo, true);
      if (!type || !table || !condition) {
        throw new CompilationError(
          `Invalid JOIN clause, expected: T1 { [INNER] | { LEFT | RIGHT | FULL } [OUTER] } JOIN T2 ON boolean_expression found: ${this.content(node)}`,
          condition,
        );
      }

      const additionalTables = source.getChildren("Other").map((other) => {
        const table = new TableExpression(
          parseInfo,
          this.content(other.getChild("Identifier")),
          this.getAlias(other),
        );
        return table.alias
          ? new TargetExpression(table.parseInfo, table, table.alias)
          : table;
      });

      if (additionalTables.length && type !== "CROSS") {
        throw new CompilationError(
          `syntax error at or near ","\n, ${this.content(source.getChild("Other"))}`,
          condition,
        );
      }

      this.lastQuery.joins.push(
        new JoinExpression(parseInfo, type, table, condition),
      );

      additionalTables.forEach((t) => {
        this.lastQuery.joins.push(
          new JoinExpression(t.parseInfo, type, t, condition),
        );
      });
    }
  }

  protected getAlias(node?: SyntaxNodeRef | SyntaxNode): string {
    if (node?.name === "As") {
      const el =
        node.node.getChild("Identifier") ?? node.node.getChild("String");
      if (el) {
        return readString(this.content(el));
      } else {
        return readString(this.content(node))
          .replaceAll(/^\s*AS/gi, "")
          .trim();
      }
    } else {
      const el = node.node.getChild("As");
      if (el) {
        return this.getAlias(el);
      }
    }
    return "";
  }

  protected add(expression: Expression) {
    this.stack.push(expression);
  }

  protected content(pos?: { from: number; to: number } | null) {
    if (!pos) {
      return "";
    }
    return this.qs.substring(pos.from, pos.to);
  }
}
