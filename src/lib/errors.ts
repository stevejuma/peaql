import { Expression } from "./parser";
import { PreparedStatment } from "./query";

interface ErrorOptions {
  cause?: unknown;
}

/** Exception raised for important warnings. */
export class Warning extends Error {
  constructor(
    message: string,
    readonly options?: ErrorOptions,
  ) {
    super(message);
  }
}

/** An error related to the database interface rather than the database itself */
export class InterfaceError extends Error {
  constructor(
    message: string,
    readonly options?: ErrorOptions,
  ) {
    super(message);
  }
}

/** Exception raised for errors that are related to the database. */
export class DatabaseError extends Error {
  constructor(
    message: string,
    readonly options?: ErrorOptions,
  ) {
    super(message);
  }
}

/** An error caused by problems with the processed data */
export class DataError extends Error {
  constructor(
    message: string,
    readonly options?: ErrorOptions,
  ) {
    super(message);
  }
}

/** An error related to the database's operation */
export class OperationalError extends Error {
  constructor(
    message: string,
    readonly options?: ErrorOptions,
  ) {
    super(message);
  }
}

/** An error caused when the relational integrity of the database is affected */
export class IntegrityError extends Error {
  constructor(
    message: string,
    readonly options?: ErrorOptions,
  ) {
    super(message);
  }
}

/** An error generated when the database encounters an internal error */
export class InternalError extends Error {
  constructor(
    message: string,
    readonly expression?: Expression,
    readonly options?: ErrorOptions,
  ) {
    super(message);
  }
}

/** Exception raised for programming errors */
export class ProgrammingError extends Error {
  constructor(
    message: string,
    readonly options?: ErrorOptions,
  ) {
    super(message);
  }
}

/** A method or database API was used which is not supported by the database */
export class NotSupportedError extends Error {
  constructor(
    message: string,
    readonly options?: ErrorOptions,
  ) {
    super(message);
  }
}

export class CompilationError extends Error {
  constructor(
    message: string,
    readonly expression?: Expression,
    readonly options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export class StatementError extends Error {
  constructor(
    message: string,
    readonly statement: PreparedStatment,
    readonly options?: ErrorOptions,
  ) {
    super(message + statement.errors.join(","), options);
  }
}

export class ParseError extends Error {
  constructor(
    message: string,
    readonly options: {
      node: string;
      position: { from: number; to: number };
      content: string;
      cause?: unknown;
    },
  ) {
    super(message);
  }
}
