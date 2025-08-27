import { DateTime } from "luxon";
import { typedTupleToColumns } from "./query/nodes";
import { registerStructure, Structure } from "./query/types";

export class TimeStamp extends Structure {
  public static name = "timestamp";
  public static columns = typedTupleToColumns({
    year: Number,
    month: Number,
    day: Number,
    ordinal: Number,
    weekYear: Number,
    localWeekYear: Number,
    weekNumber: Number,
    localWeekNumber: Number,
    weekday: Number,
    localWeekday: Number,
    quarter: Number,
  });
  public static wildcardColumns: string[] = ["year", "month", "day"];
}

registerStructure(DateTime, TimeStamp);
