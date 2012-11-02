----------------------------
-- Generic measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _distinct(_measure anyarray) RETURNS TABLE (_key_index BIGINT, _key_distinct BOOLEAN) LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Index of Boundaries of Point-wise Distinct Values of a Stack of Simple Measurable Functions
 *
 * Given a list of the boundaries of an arbitrary set of simple measurable functions, sort
 * the boundaries in order of the value of the images, and then in correct topological order
 * of the preimage of the boundaries, respecting the implicit ordering of the stack of functions. 
 * Treat nulls as a value so that gaps in the functions are computed correctly. Flag all the 
 * distinct boundaries, where either the first pushed image is preceded by no images and 
 * the previous preimage was different or the last popped image has no more images and the next
 * preimage is different.
 *
 * @author Aaron Sheldon
 * @param array Stack of simple measurable function
 * @return table Indexes with flagged boundaries of distinct image values
 */
	WITH

		-- Sort the boundaries in the preimage of the simple measurable functions
		sort_data AS
		(
			SELECT 
				a0._key_index
			FROM
				generate_series(1, array_length($1, 1), 1) a0(_key_index)
			ORDER BY
				($1[a0._key_index])._value_image ASC NULLS FIRST,
				($1[a0._key_index])._key_infinite ASC NULLS FIRST,
				($1[a0._key_index])._key_finite ASC NULLS FIRST,
				($1[a0._key_index])._key_preimage ASC NULLS FIRST,
				($1[a0._key_index])._key_topology ASC NULLS FIRST,
				($1[a0._key_index])._key_operation ASC NULLS FIRST,
				a0._key_index ASC NULLS FIRST			
		)

	-- Return indexe and flag distinct boundaries
	SELECT
		a0._key_index,
		CASE
			WHEN ($1[a0._key_index])._key_infinite THEN
				0 = COALESCE(SUM(CASE WHEN ($1[a0._key_index])._key_operation THEN 1 ELSE -1 END) OVER pop_frame, 0)
			WHEN NOT ($1[a0._key_index])._key_finite THEN
				0 = COALESCE(SUM(CASE WHEN ($1[a0._key_index])._key_operation THEN 1 ELSE -1 END) OVER push_frame, 0)
			WHEN ($1[a0._key_index])._key_operation THEN
				0 = COALESCE(SUM(CASE WHEN ($1[a0._key_index])._key_operation THEN 1 ELSE -1 END) OVER push_frame, 0)
				AND
				($1[a0._key_index])._key_preimage > lag(($1[a0._key_index])._key_preimage, 1) OVER ()
			ELSE
				0 = COALESCE(SUM(CASE WHEN ($1[a0._key_index])._key_operation THEN 1 ELSE -1 END) OVER pop_frame, 0)
				AND
				($1[a0._key_index])._key_preimage < lead(($1[a0._key_index])._key_preimage, 1) OVER ()
		END _key_distinct
	FROM
		sort_data a0
	WINDOW
		push_frame AS (PARTITION BY ($1[a0._key_index])._value_image ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
		pop_frame AS (PARTITION BY ($1[a0._key_index])._value_image ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
$body$;