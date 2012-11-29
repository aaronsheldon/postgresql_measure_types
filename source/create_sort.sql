----------------------------
-- Generic measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _sort(_measure anyarray) RETURNS TABLE (_key_index BIGINT, _key_distinct BOOLEAN) LANGUAGE sql IMMUTABLE AS
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
	SELECT
		a0._key_index,
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
		END _key_distinct
	FROM
		generate_series(1::BIGINT, array_length($1, 1)::BIGINT, 1::BIGINT) a0(_key_index)
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
				a0._key_index ASC NULLS FIRST
		),
		operation_frame AS (image_frame ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
	ORDER BY
		($1[a0._key_index])._key_infinite ASC NULLS FIRST,
		($1[a0._key_index])._key_finite ASC NULLS FIRST,
		($1[a0._key_index])._key_preimage ASC NULLS FIRST,
		($1[a0._key_index])._key_topology ASC NULLS FIRST,
		($1[a0._key_index])._key_operation ASC NULLS FIRST,
		a0._key_index ASC NULLS FIRST;
$body$;