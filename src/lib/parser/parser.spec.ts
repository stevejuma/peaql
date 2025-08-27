import { describe, test, expect } from "vitest";
import { Parser } from "..";
import dedent from "dedent";

describe("Parse", () => {
  const testCases: Array<{
    query: string;
    expected: string;
  }> = [
    {
      query: `SELECT id.toFixed()`,
      expected: `
      SELECT
        id.toFixed()`,
    },
    {
      query: `SELECT id WHERE 23 < any(99,999)`,
      expected: `
      SELECT
        id
      WHERE 23 < any(99, 999)`,
    },
    {
      query: `
       SELECT
          account,
          date
        FROM postings
        WHERE root(account, 2) = 'Income:Contributions'
        AND account IN (
          SELECT account
          FROM postings
          GROUP BY account
          HAVING number(only('USD', sum(position))) > 50.0
        )
        ORDER BY date
      `,
      expected: `
        SELECT
         account,
         date
       FROM postings
       WHERE (root(account, 2) = 'Income:Contributions' AND account IN (SELECT
         account
       FROM postings
       GROUP BY account HAVING number(only('USD', sum(position))) > 50
       ))
       ORDER BY date ASC
      `,
    },
  ];

  testCases.forEach((testCase, i) => {
    test(`${i}: - ${testCase.query}`, () => {
      const parser = new Parser(testCase.query);
      // console.log(parser.query)
      expect(parser.query.toString().trim()).toEqual(
        dedent(testCase.expected).trim(),
      );
    });
  });
});
