﻿----------------------------
-- Decimal Measure Spaces --
----------------------------

CREATE OR REPLACE FUNCTION _avg(_measure numeric_numeric[]) RETURNS numeric_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic accumulate function
 *
 * @author Aaron Sheldon
 * @param numeric_numeric Stack of simple measurable functions
 * @return numeric_numeric Simple measurable function
 */
	WITH

		-- Flag redundant boundaries
		sieve_data AS
		(
			SELECT
				_key_infinite,
				_key_finite,
				_key_preimage,
				_key_topology,
				_key_operation,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_first / a0._value_zeroth = lag(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lag(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_first / a0._value_zeroth = lead(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lead(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite __key_redundant,
				a0._value_first / a0._value_zeroth _value_image
			FROM
				_accumulate_distinct($1) a0
				(
					_key_infinite BOOLEAN,
					_key_finite BOOLEAN,
					_key_preimage NUMERIC,
					_key_topology BOOLEAN,
					_key_operation BOOLEAN,
					_value_zeroth BIGINT,
					_value_first NUMERIC,
					_value_second NUMERIC
				)
		)

	-- Return distinct boundaries
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_numeric) _return
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant
$body$;

CREATE OR REPLACE FUNCTION _avg(_measure numeric_interval[]) RETURNS numeric_interval[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic accumulate function
 *
 * @author Aaron Sheldon
 * @param numeric_interval Stack of simple measurable functions
 * @return numeric_interval Simple measurable function
 */
	WITH

		-- Flag redundant boundaries
		sieve_data AS
		(
			SELECT
				_key_infinite,
				_key_finite,
				_key_preimage,
				_key_topology,
				_key_operation,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_first / a0._value_zeroth = lag(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lag(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_first / a0._value_zeroth = lead(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lead(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite __key_redundant,
				a0._value_first / a0._value_zeroth _value_image
			FROM
				_accumulate_distinct($1) a0
				(
					_key_infinite BOOLEAN,
					_key_finite BOOLEAN,
					_key_preimage NUMERIC,
					_key_topology BOOLEAN,
					_key_operation BOOLEAN,
					_value_zeroth BIGINT,
					_value_first INTERVAL,
					_value_second INTERVAL
				)
		)

	-- Return distinct boundaries
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_interval) _return
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant
$body$;

---------------------------
-- String Measure Spaces --
---------------------------

CREATE OR REPLACE FUNCTION _avg(_measure varchar_numeric[]) RETURNS varchar_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic accumulate function
 *
 * @author Aaron Sheldon
 * @param varchar_numeric Stack of simple measurable functions
 * @return varchar_numeric Simple measurable function
 */
	WITH

		-- Flag redundant boundaries
		sieve_data AS
		(
			SELECT
				_key_infinite,
				_key_finite,
				_key_preimage,
				_key_topology,
				_key_operation,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_first / a0._value_zeroth = lag(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lag(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_first / a0._value_zeroth = lead(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lead(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite __key_redundant,
				a0._value_first / a0._value_zeroth _value_image
			FROM
				_accumulate_distinct($1) a0
				(
					_key_infinite BOOLEAN,
					_key_finite BOOLEAN,
					_key_preimage CHARACTER VARYING,
					_key_topology BOOLEAN,
					_key_operation BOOLEAN,
					_value_zeroth BIGINT,
					_value_first NUMERIC,
					_value_second NUMERIC
				)
		)

	-- Return distinct boundaries
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_numeric) _return
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant
$body$;

CREATE OR REPLACE FUNCTION _avg(_measure varchar_interval[]) RETURNS varchar_interval[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic accumulate function
 *
 * @author Aaron Sheldon
 * @param varchar_interval Stack of simple measurable functions
 * @return varchar_interval Simple measurable function
 */
	WITH

		-- Flag redundant boundaries
		sieve_data AS
		(
			SELECT
				_key_infinite,
				_key_finite,
				_key_preimage,
				_key_topology,
				_key_operation,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_first / a0._value_zeroth = lag(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lag(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_first / a0._value_zeroth = lead(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lead(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite __key_redundant,
				a0._value_first / a0._value_zeroth _value_image
			FROM
				_accumulate_distinct($1) a0
				(
					_key_infinite BOOLEAN,
					_key_finite BOOLEAN,
					_key_preimage CHARACTER VARYING,
					_key_topology BOOLEAN,
					_key_operation BOOLEAN,
					_value_zeroth BIGINT,
					_value_first INTERVAL,
					_value_second INTERVAL
				)
		)

	-- Return distinct boundaries
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_interval) _return
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant
$body$;

--------------------------
-- Times Measure Spaces --
--------------------------

CREATE OR REPLACE FUNCTION _avg(_measure timestamp_numeric[]) RETURNS timestamp_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic accumulate function
 *
 * @author Aaron Sheldon
 * @param timestamp_numeric Stack of simple measurable functions
 * @return timestamp_numeric Simple measurable function
 */
	WITH

		-- Flag redundant boundaries
		sieve_data AS
		(
			SELECT
				_key_infinite,
				_key_finite,
				_key_preimage,
				_key_topology,
				_key_operation,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_first / a0._value_zeroth = lag(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lag(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_first / a0._value_zeroth = lead(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lead(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite __key_redundant,
				a0._value_first / a0._value_zeroth _value_image
			FROM
				_accumulate_distinct($1) a0
				(
					_key_infinite BOOLEAN,
					_key_finite BOOLEAN,
					_key_preimage TIMESTAMP,
					_key_topology BOOLEAN,
					_key_operation BOOLEAN,
					_value_zeroth BIGINT,
					_value_first NUMERIC,
					_value_second NUMERIC
				)
		)

	-- Return distinct boundaries
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_numeric) _return
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant
$body$;

CREATE OR REPLACE FUNCTION _avg(_measure timestamp_interval[]) RETURNS timestamp_interval[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic accumulate function
 *
 * @author Aaron Sheldon
 * @param timestamp_interval Stack of simple measurable functions
 * @return timestamp_interval Simple measurable function
 */
	WITH

		-- Flag redundant boundaries
		sieve_data AS
		(
			SELECT
				_key_infinite,
				_key_finite,
				_key_preimage,
				_key_topology,
				_key_operation,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_first / a0._value_zeroth = lag(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lag(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_first / a0._value_zeroth = lead(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lead(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite __key_redundant,
				a0._value_first / a0._value_zeroth _value_image
			FROM
				_accumulate_distinct($1) a0
				(
					_key_infinite BOOLEAN,
					_key_finite BOOLEAN,
					_key_preimage TIMESTAMP,
					_key_topology BOOLEAN,
					_key_operation BOOLEAN,
					_value_zeroth BIGINT,
					_value_first INTERVAL,
					_value_second INTERVAL
				)
		)

	-- Return distinct boundaries
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_interval) _return
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant
$body$;

-----------------------------
-- Duration Measure Spaces --
-----------------------------

CREATE OR REPLACE FUNCTION _avg(_measure interval_numeric[]) RETURNS interval_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic accumulate function
 *
 * @author Aaron Sheldon
 * @param interval_numeric Stack of simple measurable functions
 * @return interval_numeric Simple measurable function
 */
	WITH

		-- Flag redundant boundaries
		sieve_data AS
		(
			SELECT
				_key_infinite,
				_key_finite,
				_key_preimage,
				_key_topology,
				_key_operation,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_first / a0._value_zeroth = lag(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lag(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_first / a0._value_zeroth = lead(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lead(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite __key_redundant,
				a0._value_first / a0._value_zeroth _value_image
			FROM
				_accumulate_distinct($1) a0
				(
					_key_infinite BOOLEAN,
					_key_finite BOOLEAN,
					_key_preimage INTERVAL,
					_key_topology BOOLEAN,
					_key_operation BOOLEAN,
					_value_zeroth BIGINT,
					_value_first NUMERIC,
					_value_second NUMERIC
				)
		)

	-- Return distinct boundaries
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_numeric) _return
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant
$body$;

CREATE OR REPLACE FUNCTION _avg(_measure interval_interval[]) RETURNS interval_interval[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic accumulate function
 *
 * @author Aaron Sheldon
 * @param interval_interval Stack of simple measurable functions
 * @return interval_interval Simple measurable function
 */
	WITH

		-- Flag redundant boundaries
		sieve_data AS
		(
			SELECT
				_key_infinite,
				_key_finite,
				_key_preimage,
				_key_topology,
				_key_operation,
				CASE
					WHEN ($1[a0._key_index])._key_operation THEN
						COALESCE(a0._value_first / a0._value_zeroth = lag(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lag(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_first / a0._value_zeroth = lead(a0._value_first / a0._value_zeroth, 1) OVER (), a0._value_first / a0._value_zeroth IS NULL AND lead(a0._value_first / a0._value_zeroth, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite __key_redundant,
				a0._value_first / a0._value_zeroth _value_image
			FROM
				_accumulate_distinct($1) a0
				(
					_key_infinite BOOLEAN,
					_key_finite BOOLEAN,
					_key_preimage INTERVAL,
					_key_topology BOOLEAN,
					_key_operation BOOLEAN,
					_value_zeroth BIGINT,
					_value_first INTERVAL,
					_value_second INTERVAL
				)
		)

	-- Return distinct boundaries
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_interval) _return
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant
$body$;