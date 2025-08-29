import { ExternalTokenizer } from "@lezer/lr";
import {
  set,
  primary,
  key,
  _with,
  as,
  at,
  balances,
  select,
  journal,
  print,
  where,
  group,
  by,
  having,
  order,
  pivot,
  limit,
  asc,
  desc,
  distinct,
  from,
  inner,
  left,
  right,
  full,
  cross,
  anti,
  outer,
  join,
  using,
  on,
  open,
  close,
  union,
  intersect,
  all,
  except,
  clear,
  and,
  or,
  not,
  _in,
  is,
  _null,
  between,
  filter,
  partition,
  window,
  over,
  exclude,
  no,
  others,
  current,
  row,
  ties,
  rows,
  groups,
  range,
  unbounded,
  preceding,
  following,
  interval,
  timestamp,
  timestamptz,
  time,
  zone,
  _true,
  _false,
  create,
  table,
  insert,
  into,
  values,
  _case,
  when,
  then,
  _else,
  end,
  array,
  cast,
  exists,
  _if,
  constraint,
  foreign,
  references,
  unique,
  check,
} from "./bql.grammar.terms";

const keywordMap = {
  set,
  union,
  intersect,
  except,
  with: _with,
  as,
  at,
  balances,
  journal,
  print,
  select,
  where,
  group,
  by,
  having,
  order,
  pivot,
  limit,
  asc,
  desc,
  distinct,
  from,
  inner,
  left,
  right,
  full,
  cross,
  anti,
  outer,
  join,
  using,
  on,
  open,
  close,
  primary,
  key,
  clear,
  and,
  or,
  not,
  in: _in,
  is,
  null: _null,
  between,
  filter,
  partition,
  window,
  over,
  exclude,
  no,
  others,
  current,
  row,
  ties,
  rows,
  groups,
  range,
  unbounded,
  preceding,
  following,
  interval,
  timestamp,
  timestamptz,
  time,
  zone,
  true: _true,
  false: _false,
  create,
  table,
  insert,
  into,
  values,
  case: _case,
  when,
  then,
  else: _else,
  end,
  array,
  cast,
  constraint,
  if: _if,
  foreign,
  references,
  unique,
  check,
};
const newline = 10,
  carriage = 13,
  space = 32,
  tab = 9;

function readIdentifier(input, offset, delta = -1) {
  let value = "";
  while ([space, tab, carriage].includes(input.peek(offset))) {
    offset += delta;
  }
  if (input.peek(offset) < 0) {
    return [value, offset];
  }

  while (![space, tab, newline, carriage, -1].includes(input.peek(offset))) {
    value += String.fromCharCode(input.peek(offset));
    offset += delta;
  }
  if (delta < 0) {
    return [value.split("").reverse().join(""), offset];
  }
  return [value, offset];
}

export function keywords(name) {
  return keywordMap[name.toLowerCase()] ?? -1;
}

export const properties = new ExternalTokenizer((input) => {
  const [before] = readIdentifier(input, -1, -1);
  const [after, offset] = readIdentifier(input, 0, 1);

  if (
    ["NOT"].includes(before.toUpperCase()) &&
    after.toUpperCase() === "EXISTS"
  ) {
    return input.acceptToken(exists, offset);
  }

  if (
    ["SELECT", "UNION", "EXCEPT", "INTERSECT"].includes(before.toUpperCase()) &&
    after.toUpperCase() === "ALL"
  ) {
    return input.acceptToken(all, offset);
  }
});
