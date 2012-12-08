----------------------------
-- Generic measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _sort(_measure anyarray) RETURNS TABLE (_key_index BIGINT, _key_ordinal BIGINT, _value_distinct BOOLEAN) LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Sort Array Subscripts in Topological Order
 *
 * Given a list of the boundaries of an arbitrary stack of simple measurable
 * functions, sort the boundaries in the topological order of the preimage,
 * respecting the implicit ordering of the stack of functions.
 *
 * @author Aaron Sheldon
 * @param array Stack of simple measurable function
 * @return table Subscripts
 */
	WITH

		-- Assuming the array is in function order, count the number of negative infinities
		ordinal_data AS
		(
			SELECT
				a0._key_index,
				SUM(CASE WHEN ($1[a0._key_index])._key_infinite THEN 0 WHEN ($1[a0._key_index])._key_finite THEN 0 ELSE 1 END) OVER ordinal_frame _key_ordinal
			FROM
				generate_series(1::BIGINT, array_length($1, 1)::BIGINT, 1::BIGINT) a0(_key_index)
			WINDOW
				ordinal_frame AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		)

	-- Re-sort in topological, then function stack order, and mark the distinct values in the image
	SELECT
		a0._key_index,
		a0._key_ordinal,
		CASE
			WHEN ($1[a0._key_index])._key_operation THEN
				1 = SUM(CASE WHEN ($1[a0._key_index])._key_operation THEN 1 ELSE -1 END) OVER operation_frame
				AND
				(
					COALESCE(($1[a0._key_index])._key_preimage > lag(($1[a0._key_index])._key_preimage, 1) OVER image_frame, TRUE)
					OR
					COALESCE(($1[a0._key_index])._key_topology > lag(($1[a0._key_index])._key_topology, 1) OVER image_frame, TRUE)
				)
			ELSE
				0 = SUM(CASE WHEN ($1[a0._key_index])._key_operation THEN 1 ELSE -1 END) OVER operation_frame
				AND
				(
					COALESCE(($1[a0._key_index])._key_preimage < lead(($1[a0._key_index])._key_preimage, 1) OVER image_frame, TRUE)
					OR
					COALESCE(($1[a0._key_index])._key_topology < lead(($1[a0._key_index])._key_topology, 1) OVER image_frame, TRUE)
				)
		END _value_distinct
	FROM
		ordinal_data a0
	WINDOW
		image_frame AS
		(
			PARTITION BY
				($1[a0._key_index])._value_image
			ORDER BY
				($1[a0._key_index])._key_infinite ASC NULLS FIRST,
				($1[a0._key_index])._key_finite ASC NULLS FIRST,
				($1[a0._key_index])._key_preimage ASC NULLS FIRST,
				($1[a0._key_index])._key_topology ASC NULLS FIRST,
				($1[a0._key_index])._key_operation ASC NULLS FIRST,
				CASE WHEN ($1[a0._key_index])._key_operation THEN a0._key_ordinal ELSE - a0._key_ordinal END ASC NULLS FIRST
		),
		operation_frame AS (image_frame ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
	ORDER BY
		($1[a0._key_index])._key_infinite ASC NULLS FIRST,
		($1[a0._key_index])._key_finite ASC NULLS FIRST,
		($1[a0._key_index])._key_preimage ASC NULLS FIRST,
		($1[a0._key_index])._key_topology ASC NULLS FIRST,
		($1[a0._key_index])._key_operation ASC NULLS FIRST,
		CASE WHEN ($1[a0._key_index])._key_operation THEN a0._key_ordinal ELSE - a0._key_ordinal END ASC NULLS FIRST;
$body$;