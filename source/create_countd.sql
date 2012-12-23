----------------------------
-- Generic measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _countd_implement(_measure anyarray) RETURNS SETOF RECORD LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Pointwise Not Null Count of Simple Measurable Functions
 *
 * Find the count on each boundary by cumulatively adding 1 for every not null pushed and
 * substracting 1 popped for every not null popped. The final value on popped boundary is the value
 * last cumulative value last pushed, and the final value on a pushed boundary is the last
 * value pushed. Sieve out all popped boundaries sequentially unbroken following the first popped
 * boundary as degenerate/redundant, and all pushed boundaries sequential unbroken preceding the
 * last pushed boundary. Sorting on the index ensures the pre-aggregate ordering of the simple
 * measurable functions is respected.
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
								WHEN ($1[a0._key_index])._key_operation AND ($1[a0._key_index])._value_image IS NOT NULL AND a0._value_distinct THEN
									1
								WHEN ($1[a0._key_index])._value_image IS NOT NULL AND a0._value_distinct THEN
									- 1
								ELSE
									0
							END
						) OVER push_frame
					ELSE
						sum
						(
							CASE
								WHEN ($1[a0._key_index])._key_operation AND ($1[a0._key_index])._value_image IS NOT NULL AND a0._value_distinct THEN
									1
								WHEN ($1[a0._key_index])._value_image IS NOT NULL AND a0._value_distinct THEN
									- 1
								ELSE
									0
							END
						) OVER pop_frame
				END _value_image
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
						COALESCE(a0._value_image = lag(a0._value_image, 1) OVER (), a0._value_image IS NULL AND lag(a0._value_image, 1) OVER () IS NULL)
 					ELSE
						COALESCE(a0._value_image = lead(a0._value_image, 1) OVER (), a0._value_image IS NULL AND lead(a0._value_image, 1) OVER () IS NULL)
				END
				AND
				($1[a0._key_index])._key_finite _key_redundant,
				a0._value_image
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
		a0._value_image::BIGINT
	FROM
		sieve_data a0
	WHERE
		NOT a0._key_redundant;
$body$;

----------------------------
-- Decimal Measure Spaces --
----------------------------

CREATE OR REPLACE FUNCTION _countd(_measure numeric_numeric[]) RETURNS numeric_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param numeric_numeric Stack of simple measurable functions
 * @return numeric_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage NUMERIC,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure numeric_varchar[]) RETURNS numeric_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param numeric_varchar Stack of simple measurable functions
 * @return numeric_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage NUMERIC,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure numeric_timestamp[]) RETURNS numeric_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param numeric_timestamp Stack of simple measurable functions
 * @return numeric_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage NUMERIC,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure numeric_interval[]) RETURNS numeric_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param numeric_interval Stack of simple measurable functions
 * @return numeric_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage NUMERIC,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

---------------------------
-- String Measure Spaces --
---------------------------

CREATE OR REPLACE FUNCTION _countd(_measure varchar_numeric[]) RETURNS varchar_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param varchar_numeric Stack of simple measurable functions
 * @return varchar_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage CHARACTER VARYING,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure varchar_varchar[]) RETURNS varchar_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param varchar_varchar Stack of simple measurable functions
 * @return varchar_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage CHARACTER VARYING,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure varchar_timestamp[]) RETURNS varchar_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param varchar_timestamp Stack of simple measurable functions
 * @return varchar_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage CHARACTER VARYING,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure varchar_interval[]) RETURNS varchar_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param varchar_interval Stack of simple measurable functions
 * @return varchar_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage CHARACTER VARYING,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

--------------------------
-- Times Measure Spaces --
--------------------------

CREATE OR REPLACE FUNCTION _countd(_measure timestamp_numeric[]) RETURNS timestamp_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param timestamp_numeric Stack of simple measurable functions
 * @return timestamp_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage TIMESTAMP,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure timestamp_varchar[]) RETURNS timestamp_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param timestamp_varchar Stack of simple measurable functions
 * @return timestamp_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage TIMESTAMP,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure timestamp_timestamp[]) RETURNS timestamp_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param timestamp_timestamp Stack of simple measurable functions
 * @return timestamp_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage TIMESTAMP,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure timestamp_interval[]) RETURNS timestamp_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param timestamp_interval Stack of simple measurable functions
 * @return timestamp_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage TIMESTAMP,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

-----------------------------
-- Duration Measure Spaces --
-----------------------------

CREATE OR REPLACE FUNCTION _countd(_measure interval_numeric[]) RETURNS interval_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param interval_numeric Stack of simple measurable functions
 * @return interval_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage INTERVAL,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure interval_varchar[]) RETURNS interval_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param interval_varchar Stack of simple measurable functions
 * @return interval_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage INTERVAL,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure interval_timestamp[]) RETURNS interval_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param interval_timestamp Stack of simple measurable functions
 * @return interval_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage INTERVAL,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

CREATE OR REPLACE FUNCTION _countd(_measure interval_interval[]) RETURNS interval_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Assert input type matches output type when calling generic count function
 *
 * @author Aaron Sheldon
 * @param interval_interval Stack of simple measurable functions
 * @return interval_numeric Simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_numeric) _return
	FROM
		_countd_implement($1) a0
		(
			_key_infinite BOOLEAN,
			_key_finite BOOLEAN,
			_key_preimage INTERVAL,
			_key_topology BOOLEAN,
			_key_operation BOOLEAN,
			_value_image BIGINT
		)
$body$;

----------------------
-- Final Aggregates --
----------------------

CREATE AGGREGATE countd(numeric_numeric[])
(
	sfunc = array_cat,
	stype = numeric_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(numeric_varchar[])
(
	sfunc = array_cat,
	stype = numeric_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(numeric_timestamp[])
(
	sfunc = array_cat,
	stype = numeric_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(numeric_interval[])
(
	sfunc = array_cat,
	stype = numeric_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(varchar_numeric[])
(
	sfunc = array_cat,
	stype = varchar_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(varchar_varchar[])
(
	sfunc = array_cat,
	stype = varchar_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(varchar_timestamp[])
(
	sfunc = array_cat,
	stype = varchar_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(varchar_interval[])
(
	sfunc = array_cat,
	stype = varchar_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(timestamp_numeric[])
(
	sfunc = array_cat,
	stype = timestamp_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(timestamp_varchar[])
(
	sfunc = array_cat,
	stype = timestamp_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(timestamp_timestamp[])
(
	sfunc = array_cat,
	stype = timestamp_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(timestamp_interval[])
(
	sfunc = array_cat,
	stype = timestamp_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(interval_numeric[])
(
	sfunc = array_cat,
	stype = interval_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(interval_varchar[])
(
	sfunc = array_cat,
	stype = interval_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(interval_timestamp[])
(
	sfunc = array_cat,
	stype = interval_numeric[],
	ffunc = _countd
);

CREATE AGGREGATE countd(interval_interval[])
(
	sfunc = array_cat,
	stype = interval_numeric[],
	ffunc = _countd
);