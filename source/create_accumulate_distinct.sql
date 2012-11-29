----------------------------
-- Generic measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _accumulate_distinct(_measure anyarray) RETURNS SETOF RECORD LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Pointwise Zeroth, First, and Second Order Sums of Simple Measurable Functions
 *
 * Find zeroth, first, and second order sums by cumulatively pushing and popping distinct values to 
 * the sum. The final value on popped boundary is the value last cumulative value last pushed, 
 * and the final value on a pushed boundary is the last value pushed. Sieve out all popped 
 * boundaries sequentially unbroken following the first popped boundary as degenerate/redundant, 
 * and all pushed boundaries sequential unbroken preceding the last pushed boundary. Sorting on
 * the index ensures the pre-aggregate ordering of the simple measurable functions is respected.
 *
 * @author Aaron Sheldon
 * @param array Stack of simple measurable function
 * @return table Representation of a generic measurable function
 */
	WITH

		-- Calculated the image at the boundary, asserting popped equals previous pushed
		compute_data AS
		(
			SELECT
				a0._key_index,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						($1[a0._key_index])._key_operation = lead(($1[a0._key_index])._key_operation, 1) OVER ()
					ELSE
						($1[a0._key_index])._key_operation = lag(($1[a0._key_index])._key_operation, 1) OVER ()
				END _key_degenerate,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						sum
						(
							CASE 
								WHEN ($1[a0._key_index])._key_operation AND ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									1 
								WHEN ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									- 1 
								ELSE 
									0
							END
						) OVER push_frame
					ELSE
						sum
						(
							CASE 
								WHEN ($1[a0._key_index])._key_operation AND ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									1 
								WHEN ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									- 1 
								ELSE 
									0
							END
						) OVER pop_frame
				END _value_zeroth,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						sum
						(
							CASE 
								WHEN ($1[a0._key_index])._key_operation AND ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									($1[a0._key_index])._value_image 
								WHEN ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									- ($1[a0._key_index])._value_image 
								ELSE 
									0
							END
						) OVER push_frame
					ELSE
						sum
						(
							CASE 
								WHEN ($1[a0._key_index])._key_operation AND ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									($1[a0._key_index])._value_image 
								WHEN ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									- ($1[a0._key_index])._value_image 
								ELSE 
									0
							END
						) OVER pop_frame
				END _value_first,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						sum
						(
							CASE 
								WHEN ($1[a0._key_index])._key_operation AND ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									($1[a0._key_index])._value_image * ($1[a0._key_index])._value_image 
								WHEN ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									- ($1[a0._key_index])._value_image * ($1[a0._key_index])._value_image 
								ELSE 
									0
							END
						) OVER push_frame
					ELSE
						sum
						(
							CASE 
								WHEN ($1[a0._key_index])._key_operation AND ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									($1[a0._key_index])._value_image * ($1[a0._key_index])._value_image 
								WHEN ($1[a0._key_index])._value_image IS NOT NULL AND a0._key_distinct THEN 
									- ($1[a0._key_index])._value_image * ($1[a0._key_index])._value_image 
								ELSE 
									0
							END
						) OVER pop_frame
				END _value_second
			FROM
				_sort($1) a0
			WINDOW
				push_frame AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
				pop_frame AS (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
		),	

		-- Prune degenerate boundaries, and flag redundant boundaries
		sieve_data AS
		(
			SELECT
				a0._key_index,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_zeroth = lag(a0._value_zeroth, 1) OVER (), a0._value_zeroth IS NULL AND lag(a0._value_zeroth, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_zeroth = lead(a0._value_zeroth, 1) OVER (), a0._value_zeroth IS NULL AND lead(a0._value_zeroth, 1) OVER () IS NULL)
				END
				AND
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_first = lag(a0._value_first, 1) OVER (), a0._value_first IS NULL AND lag(a0._value_first, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_first = lead(a0._value_first, 1) OVER (), a0._value_first IS NULL AND lead(a0._value_first, 1) OVER () IS NULL)
				END
				AND
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_second = lag(a0._value_second, 1) OVER (), a0._value_second IS NULL AND lag(a0._value_second, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_second = lead(a0._value_second, 1) OVER (), a0._value_second IS NULL AND lead(a0._value_second, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite _key_redundant,
				a0._value_zeroth,
				CASE
					WHEN a0._value_zeroth > 0 THEN
						a0._Value_first
					ELSE
						NULL
				END _value_first,
				CASE
					WHEN a0._value_zeroth > 0 THEN
						a0._value_second
					ELSE
						NULL
				END _value_second
			FROM
				compute_data a0
			WHERE
				NOT a0._key_degenerate
		)	

	-- Return generic records of distinct boundaries
	SELECT
		($1[a0._key_index])._key_infinite::BOOLEAN,
		($1[a0._key_index])._key_finite::BOOLEAN,
		($1[a0._key_index])._key_preimage,
		($1[a0._key_index])._key_topology::BOOLEAN,
		($1[a0._key_index])._key_operation::BOOLEAN,
		a0._value_zeroth::BIGINT,
		a0._value_first,
		a0._value_second
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant;
$body$