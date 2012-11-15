----------------------------
-- Generic measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _distinct(_measure anyarray) RETURNS TABLE (_key_index BIGINT, _key_distinct BOOLEAN) LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Index of Boundaries of Point-wise Distinct Values of a Stack of Simple Measurable Functions
 *
 * Treat nulls as a value so that gaps in the functions are computed correctly. Flag all the 
 * distinct boundaries, where either the first pushed image is preceded by no images and 
 * the previous preimage was different, or the last popped image has no more images and the next
 * preimage is different.
 *
 * @author Aaron Sheldon
 * @param array Stack of simple measurable function
 * @return table Indexes of distinct image values
 */
	WITH

		-- Topological order the boundaries and mark the distinct values
	sieve_data AS
	(
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
			generate_subscripts(1::BIGINT, array_length($1, 1)::BIGINT, 1::BIGINT) a0(_key_index)
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
			a0._key_index ASC NULLS FIRST
	)

-- Return indexes of distinct boundaries	
SELECT
	a0._key_index
FROM
	sieve_data a0
WHERE
	a0._key_distinct;
$body$;