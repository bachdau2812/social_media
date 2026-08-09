# Music Popularity Search Design

## Goal

Expose the existing `musics.popularity DECIMAL(6,5) UNSIGNED NULL` column through the music API and rank every music-list result by popularity descending.

## Data mapping

- Add `BigDecimal popularity` to the `Musics` entity.
- Keep the existing direct `Musics` response contract; no new DTO or mapper is introduced.
- Nullable database values remain nullable in Java and JSON.

## Query behavior

Apply `ORDER BY popularity DESC` to all four repository list queries:

- unfiltered catalog;
- category-only catalog;
- keyword-only music search;
- keyword-and-category music search.

Popularity is the only ordering expression. Do not add `display_name`, `id`, or another secondary ordering column. MySQL places null popularity values after non-null values for descending order.

Count queries remain unchanged because ordering does not affect counts.

## Compatibility and scope

- Preserve the existing prefix search pattern (`query%`) and current pagination flow.
- Do not change the controller route, request parameters, response envelope, frontend, fetch coordinator, or backend API contract.
- Preserve unrelated uncommitted changes in the repository, including the existing prefix-LIKE and SSE work.

## Verification

- A source-level repository contract test must fail before the SQL change and then verify all four list queries use only `popularity DESC`.
- An entity mapping test must verify that popularity uses `BigDecimal`.
- Run focused media tests and a Maven package build after implementation.
