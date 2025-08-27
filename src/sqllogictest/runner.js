/* eslint-disable */
import parser from "./parser";

export function parseCommands(contents, options = {}) {
  const commands = [];
  const textCommands = contents
    .replace(/#\n/g, "#") // remove comments
    .replace(/#[^\n]*/g, "") // remove comments
    .replace(/\r/g, "") // remove \r so can focus on LF and not CR? LF
    .replace(/\n{3,}/g, "\n\n") // Make sure all double+ linespaces are uniform
    .trim() // Trim the string so we dont get empty elements first and last
    .split("\n\n"); // Make array with one command in each chunck

  for (let i = 0; i < textCommands.length; i++) {
    if ("" === textCommands[i]) {
      continue;
    }
    try {
      commands.push(parser.parse(textCommands[i] + "\n"));
    } catch (e) {
      // output if could not be passed
      console.log(
        "************ Error parseing test number",
        i + 1,
        "in file",
        options.path,
      );
      console.log("previus one (passed):", textCommands[i - 1]);
      console.log("this one (failed):", textCommands[i]);
      if (i + 1 < textCommands.length)
        console.log("Next test to be passed:", textCommands[i + 1]);
      console.log("");
      console.log(JSON.stringify({ error: e.message }));
      console.log("----------------");
      console.log("");
    }
  }
  return commands;
}

export function cleanResults(result, sortType) {
  if (!result) {
    return result;
  }

  if (!result.length) {
    return result;
  }

  if (!result[0].length) {
    return result;
  }

  for (var i = 0; i < result.length; i++) {
    result[i] = result[i].map(function (x) {
      if (true === x) {
        return "1";
      }

      if (false === x) {
        return "0";
      }

      if (null === x) {
        return "NULL";
      }

      if ("Infinity" === "" + x) {
        return "NULL";
      }

      if ("-Infinity" === "" + x) {
        return "NULL";
      }

      if ("NaN" === "" + x) {
        return "NULL";
      }

      if ("undefined" === "" + x) {
        return "NULL";
      }

      if ("" === x) {
        return "(empty)";
      }

      // Its a float
      if (x === +x && x !== (x | 0)) {
        return "" + x.toFixed(3);
      }

      // remove printable chars
      return ("" + x).replace(/[\n\r\t\x00\x08\x0B\x0C\x0E-\x1F\x7F]/gim, "@");
    });
  }
  //   console.log(result)

  if ("rowsort" === sortType) {
    result.sort();
  }

  result = [].concat.apply([], result);

  if ("valuesort" === sortType) {
    result.sort();
  }

  return result;
}
