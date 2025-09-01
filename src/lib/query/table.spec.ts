import { describe, test, expect } from "vitest";
import { Context } from "./context";
import { Table } from "./models";

const EXPECTED = {
  name: "album",
  columns: [
    {
      name: "album_id",
      type: "integer",
    },
    {
      name: "title",
      type: "string",
    },
    {
      name: "artist_id",
      type: "integer",
    },
  ],
  constraints: [
    {
      column: "album_id",
      expr: "album_id IS NOT NULL",
      name: "not-null",
    },
    {
      column: "title",
      expr: "title IS NOT NULL",
      name: "not-null",
    },
    {
      column: "artist_id",
      expr: "artist_id IS NOT NULL",
      name: "not-null",
    },
    {
      column: "artist_id",
      expr: "artist_id > 0",
      name: "album_artist_id_check",
    },
    {
      column: undefined,
      expr: "length(title) > 0",
      name: "name_not_empty",
    },
    {
      column: undefined,
      expr: "artist_id < 10",
      name: "album_artist_id_check",
    },
  ],
  data: [
    {
      album_id: 1,
      artist_id: 1,
      title: "For Those About To Rock We Salute You",
    },
    {
      album_id: 2,
      artist_id: 2,
      title: "Balls to the Wall",
    },
  ],
};

describe("Table", () => {
  test(".toJSON()", () => {
    const context = Context.create();
    context.execute(`
CREATE TABLE album
(
    album_id INT NOT NULL,
    title VARCHAR(160) NOT NULL,
    artist_id INT NOT NULL CHECK (artist_id > 0),
    CONSTRAINT album_pkey PRIMARY KEY  (album_id),
    CONSTRAINT FOREIGN KEY (artist_id) REFERENCES artist (artist_id),
    CONSTRAINT name_not_empty CHECK(length(title) > 0),
    CHECK (artist_id < 10)
);

INSERT INTO album (album_id, title, artist_id) VALUES
(1, 'For Those About To Rock We Salute You', 1),
(2, 'Balls to the Wall', 2);
    `);
    expect(context.tables.get("album").toJSON()).toEqual(EXPECTED);
  });

  test(".fromJSON()", () => {
    expect(Table.fromJSON(EXPECTED).toJSON()).toEqual(EXPECTED);
  });
});
