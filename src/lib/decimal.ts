import Big from "big.js";
export type NumberSource = number | string | Decimal | null | undefined;
/** RoundDown = 0, RoundHalfUp = 1, RoundHalfEven = 2, RoundUp = 3 */
export type RoundingMode = 0 | 1 | 2 | 3;

export function isValidNumber(value: string) {
  value = value.trim().replaceAll(/[,]/g, "");
  return /^-?(\d+(\.\d*)?|\.\d+)(e[+-]?\d+)?$/i.test(value);
}

export function parseNumber(value: string | undefined): Decimal | null {
  if (!value) {
    return null;
  }
  value = value.trim().replaceAll(/[,]/g, "");
  if (!/^-?(\d+(\.\d*)?|\.\d+)(e[+-]?\d+)?$/i.test(value)) {
    throw new Error(`Invalid number: ${value}`);
  }
  return new Decimal(value);
}

export function countDecimalPlaces(input: number | string): number {
  // Convert input to a string (if it's not already)
  const numStr: string = input.toString();

  // If the number is in exponential notation, convert it to fixed-point notation
  if (numStr.includes("e")) {
    const [base, exponent] = numStr.split("e");
    const decimalPlacesInBase: number = (base.split(".")[1] || "").length;
    const exponentValue: number = Math.abs(Number(exponent));

    // The total number of decimal places is the number of decimal places in the base
    // minus the magnitude of the exponent (if negative)
    return Math.max(0, decimalPlacesInBase - exponentValue);
  } else if (numStr.includes(".")) {
    // For non-exponential numbers, handle as usual
    return numStr.split(".")[1].length;
  } else {
    // No decimal places for integers
    return 0;
  }
}

export class Decimal {
  static readonly ZERO: Decimal = new Decimal("0.00");
  static readonly ONE: Decimal = new Decimal("1.00");

  private num: Big | undefined;
  private source: string;

  /**
   * Constructor from a number and currency.
   *
   * @param value A Decimal instance.
   * @param currency A string, the currency symbol to use.
   */
  constructor(value: NumberSource, precision?: number) {
    if (typeof value === "string") {
      if (value === "") {
        this.source = "";
        this.num = undefined;
      } else {
        if (!isValidNumber(value)) {
          throw new Error(`Invalid number: ${value}`);
        }
        this.source = value;
        this.num = Big(value);
      }
    } else if (typeof value === "number") {
      this.source = value.toString();
      this.num = Big(value);
    } else if (value instanceof Decimal) {
      this.source = value.source;
      this.num = value.num;
    } else {
      this.source = "";
      this.num = undefined;
    }

    if (precision && this.num && this.precision < precision) {
      this.source = this.toFixed(Math.abs(precision));
      this.num = new Big(this.source);
    }
  }

  toJSON() {
    return this.toString();
  }

  valueOf() {
    if (this.num === undefined) {
      return undefined;
    }
    return this.num.toNumber();
  }

  digits(): Array<number> {
    return this.num?.c || [];
  }

  quantize(value: NumberSource, roundingMode?: RoundingMode) {
    if (this.num === undefined) {
      throw new Error("Missing units on number");
    }

    const num = this.resolve(value);
    const e = num.exponent;
    if (e < 0) {
      return new Decimal(this.num.toFixed(Math.abs(e), roundingMode));
    }
    return new Decimal(this.num.toFixed());
  }

  public copy(
    data: Partial<{
      value: string | number;
    }> = {},
  ) {
    return new Decimal(data.value || this.value, this.precision);
  }

  get zero() {
    if (this.num === undefined) {
      throw new Error("Missing units on number");
    }
    return this.num.eq(Big(0));
  }

  get value() {
    return this.num?.toString() || "";
  }

  get number() {
    if (this.num === undefined) {
      throw new Error("Missing units on number");
    }
    return this.num.toNumber();
  }

  get exponent() {
    if (this.num === undefined) {
      throw new Error("Missing units on number");
    }
    return countDecimalPlaces(this.source || this.value) * -1;
  }

  get precision() {
    if (this.num === undefined) {
      throw new Error("Missing units on number");
    }
    return countDecimalPlaces(this.source || this.value);
  }

  public scaleb(exponent: number) {
    return new Decimal(this.value).mul(Math.pow(10, exponent));
  }

  toFixed(dp?: number, roundingMode?: RoundingMode) {
    return this.num?.toFixed(dp, roundingMode);
  }

  toScale(dp?: number, roundingMode?: RoundingMode) {
    return new Decimal(this.toFixed(dp, roundingMode));
  }

  public eq(amount: NumberSource) {
    if (this.num === undefined) {
      return amount === undefined ? true : false;
    }
    if (amount === null || amount === undefined) {
      return false;
    }
    const value = this.resolve(amount);
    if (value.num === undefined) {
      return false;
    }
    return this.num.eq(value.num);
  }

  public plus(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }

    return new Decimal(this.num.plus(value.num).toString(), this.precision);
  }

  public minus(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }
    return new Decimal(this.num.minus(value.num).toString(), this.precision);
  }

  public lt(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }
    return this.num.lt(value.num);
  }

  public lte(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }
    return this.num.lte(value.num);
  }

  public gt(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }
    return this.num.gt(value.num);
  }

  public gte(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }
    return this.num.gte(value.num);
  }

  public mul(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }
    return new Decimal(this.num.mul(value.num).toString(), this.precision);
  }

  public div(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }
    return new Decimal(this.num.div(value.num).toString(), this.precision);
  }

  public add(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }
    return new Decimal(this.num.add(value.num).toString(), this.precision);
  }

  public sub(amount: NumberSource) {
    const value = this.resolve(amount);
    if (this.num === undefined || value.num === undefined) {
      throw new Error("Missing units on number");
    }
    return new Decimal(this.num.sub(value.num).toString(), this.precision);
  }

  public neg() {
    if (this.num === undefined) {
      throw new Error("Missing units on number");
    }
    return new Decimal(this.num.neg().toString(), this.precision);
  }

  public abs() {
    if (this.num === undefined) {
      throw new Error("Missing units on number");
    }
    return new Decimal(this.num.abs().toString(), this.precision);
  }

  private resolve(amount: NumberSource) {
    return new Decimal(amount, this.precision);
  }

  public toString(numeric: boolean = false) {
    if (this.source && !numeric) {
      return this.source.trim();
    }
    return this.num ? this.num.toString() : "";
  }

  static decimals(...numbers: NumberSource[]) {
    return numbers
      .filter((it) => !(it === null || it === undefined || it === ""))
      .map((number) => {
        return number instanceof Decimal ? number : new Decimal(number);
      });
  }

  static max(...numbers: NumberSource[]): Decimal {
    const decimals = Decimal.decimals(...numbers);
    let value: Decimal = decimals[0];
    for (const num of decimals) {
      if (num.gt(value)) {
        value = num;
      }
    }
    return value;
  }

  static min(...numbers: NumberSource[]): Decimal {
    const decimals = Decimal.decimals(...numbers);
    let value: Decimal = decimals[0];
    for (const num of decimals) {
      if (num.lt(value)) {
        value = num;
      }
    }
    return value;
  }
}
