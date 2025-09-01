import { describe, test, expect } from "vitest";
import { Context } from "./query";
import demoSQL from "./demo.spec.sql?raw";

describe("chinook dataset queries", () => {
  const context = Context.create();
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

  test("count (distinct...)", () => {
    const [_, data] = context.execute(`
select
  playlist.name,
  count(distinct artist.artist_id)
from
  playlist
  join playlist_track pt on pt.playlist_id = playlist.playlist_id
  join track on track.track_id = pt.track_id
  join album on album.album_id = track.album_id
  join artist on artist.artist_id = album.artist_id
group by
  1
order by
  2 desc, 1
    `);
    expect(data).toEqual([
      ["Music", 198],
      ["90’s Music", 109],
      ["Classical", 67],
      ["Classical 101 - Deep Cuts", 25],
      ["Classical 101 - The Basics", 25],
      ["Classical 101 - Next Steps", 23],
      ["Brazilian Music", 12],
      ["Heavy Metal Classic", 9],
      ["Grunge", 6],
      ["TV Shows", 6],
      ["Music Videos", 1],
      ["On-The-Go 1", 1],
    ]);
  });
});
