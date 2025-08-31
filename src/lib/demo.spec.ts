import { describe, test, expect } from "vitest";
import { Context } from "./query";
import demoSQL from "./demo.spec.sql?raw";

describe("chinook dataset queries", () => {
  const context = new Context();
  context.execute(demoSQL);
  test("Equi-join", () => {
    const [_, data] = context.execute(`
select
  playlist.name,
  count(pt.track_id)
from
  playlist
  join playlist_track pt on pt.playlist_id = playlist.playlist_id
  join track on track.track_id = pt.track_id
group by
  1
order by
  2 desc
limit
  10;    
    `);
    expect(data).toEqual([
      ["Music", 6580],
      ["90’s Music", 1477],
      ["TV Shows", 426],
      ["Classical", 75],
      ["Brazilian Music", 39],
      ["Heavy Metal Classic", 26],
      ["Classical 101 - Deep Cuts", 25],
      ["Classical 101 - Next Steps", 25],
      ["Classical 101 - The Basics", 25],
      ["Grunge", 15],
    ]);
  });
});
