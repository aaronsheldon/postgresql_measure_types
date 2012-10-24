----------------------------
-- Generic measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _accumulate(_measure anyarray) RETURNS SETOF RECORD LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Pointwise Zeroth, First, and Second Order Sums of Simple Measurable Functions
 *
 * Given a list of the boundaries of an arbitrary set of simple measurable functions, sort
 * the boundaries in correct topological order. Find zeroth, first, and second order sums by
 * cumulatively pushing and popping values to the sum. The final value on popped boundary is 
 * the value last cumulative value last pushed, and the final value on a pushed boundary is the
 * last value pushed. Sieve out all popped boundaries sequentially unbroken following the first
 * popped boundary as degenerate/redundant, and all pushed boundaries sequential unbroken
 * preceding the last pushed boundary. Sorting on the index ensures the pre-aggregate ordering 
 * of the simple measurable functions is respected.
 *
 * @author Aaron Sheldon
 * @param array Stack of simple measurable function
 * @return table Representation of a generic measurable function
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
				($1[a0._key_index])._key_infinite ASC NULLS FIRST,
				($1[a0._key_index])._key_finite ASC NULLS FIRST,
				($1[a0._key_index])._key_preimage ASC NULLS FIRST,
				($1[a0._key_index])._key_topology ASC NULLS FIRST,
				($1[a0._key_index])._key_operation ASC NULLS FIRST,
				a0._key_index ASC NULLS FIRST			
		),

		-- Find the sum at each boundary, pushing adds to the sum, popping subtracts from the sum
		compute_data AS
		(
			SELECT
				a0._key_index,
				($1[a0._key_index])._key_operation,
				sum
				(
					CASE 
						WHEN ($1[a0._key_index])._key_operation AND ($1[a0._key_index])._value_image IS NOT NULL THEN 
							1 
						WHEN ($1[a0._key_index])._value_image IS NOT NULL THEN 
							- 1 
						ELSE 
							0
					END
				) OVER accumulate_frame _value_zeroth,
				sum
				(
					CASE 
						WHEN ($1[a0._key_index])._key_operation THEN 
							($1[a0._key_index])._value_image 
						ELSE 
							- ($1[a0._key_index])._value_image 
					END
				) OVER accumulate_frame _value_first,
				sum
				(
					CASE 
						WHEN ($1[a0._key_index])._key_operation THEN 
							($1[a0._key_index])._value_image * ($1[a0._key_index])._value_image
						ELSE 
							- ($1[a0._key_index])._value_image * ($1[a0._key_index])._value_image
					END
				) OVER accumulate_frame _value_second
			FROM
				sort_data a0
			WINDOW
				accumulate_frame AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		),	

		-- Flag degenerate boundaries, and assert popped equals pushed
		degenerate_data AS
		(
			SELECT
				a0._key_index,
				a0._key_operation,
				CASE
					WHEN a0._key_operation THEN
						a0._key_operation = lead(a0._key_operation, 1) OVER lead_frame
					ELSE
						a0._key_operation = lag(a0._key_operation, 1) OVER lag_frame
				END _key_degenerate,
				CASE
					WHEN a0._key_operation THEN
						a0._value_zeroth
					ELSE
						lag(a0._value_zeroth, 1) OVER lag_frame
				END _value_zeroth,
				CASE
					WHEN a0._key_operation THEN
						a0._value_first
					ELSE
						lag(a0._value_first, 1) OVER lag_frame
				END _value_first,
				CASE
					WHEN a0._key_operation THEN
						a0._value_second
					ELSE
						lag(a0._value_second, 1) OVER lag_frame
				END _value_second
			FROM
				compute_data a0
			WINDOW
				lag_frame AS (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
				lead_frame AS (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
		),

		-- Flag redundant boundaries, where the image is the same on either side
		redundant_data AS
		(
			SELECT
				a0._key_index,
				CASE
					WHEN a0._key_operation THEN
						COALESCE(a0._value_zeroth = lag(a0._value_zeroth, 1) OVER lag_frame, a0._value_zeroth IS NULL AND lag(a0._value_zeroth, 1) OVER lag_frame IS NULL)
 					ELSE
						COALESCE(a0._value_zeroth = lead(a0._value_zeroth, 1) OVER lead_frame, a0._value_zeroth IS NULL AND lead(a0._value_zeroth, 1) OVER lead_frame IS NULL)
				END
				AND
				CASE
					WHEN a0._key_operation THEN
						COALESCE(a0._value_first = lag(a0._value_first, 1) OVER lag_frame, a0._value_first IS NULL AND lag(a0._value_first, 1) OVER lag_frame IS NULL)
 					ELSE
						COALESCE(a0._value_first = lead(a0._value_first, 1) OVER lead_frame, a0._value_first IS NULL AND lead(a0._value_first, 1) OVER lead_frame IS NULL)
				END
				AND
				CASE
					WHEN a0._key_operation THEN
						COALESCE(a0._value_second = lag(a0._value_second, 1) OVER lag_frame, a0._value_second IS NULL AND lag(a0._value_second, 1) OVER lag_frame IS NULL)
 					ELSE
						COALESCE(a0._value_second = lead(a0._value_second, 1) OVER lead_frame, a0._value_second IS NULL AND lead(a0._value_second, 1) OVER lead_frame IS NULL)
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
					WHEN a0._value_second > 0 THEN
						a0._value_second
					ELSE
						NULL
				END _value_second
			FROM
				degenerate_data a0
			WHERE
				NOT a0._key_degenerate
			WINDOW
				lag_frame AS (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
				lead_frame AS (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
		)

	-- Return generic records of distinct non-degenerate/redundant boundaries
	SELECT
		($1[a0._key_index])._key_infinite,
		($1[a0._key_index])._key_finite,
		($1[a0._key_index])._key_preimage,
		($1[a0._key_index])._key_topology,
		($1[a0._key_index])._key_operation,
		a0._value_zeroth,
		a0._value_first,
		a0._value_second
	FROM
		redundant_data a0
	WHERE
		NOT a0._key_redundant;
$body$