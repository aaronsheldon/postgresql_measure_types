----------------------------
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
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_first::NUMERIC / a0._value_zeroth::BIGINT)::numeric_numeric) _return
	FROM
		_accumulate($1) a0
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
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_first::INTERVAL / a0._value_zeroth::BIGINT)::numeric_interval) _return
	FROM
		_accumulate($1) a0
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
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_first::NUMERIC / a0._value_zeroth::BIGINT)::varchar_numeric) _return
	FROM
		_accumulate($1) a0
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
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_first::INTERVAL / a0._value_zeroth::BIGINT)::varchar_interval) _return
	FROM
		_accumulate($1) a0
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
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_first::NUMERIC / a0._value_zeroth::BIGINT)::timestamp_numeric) _return
	FROM
		_accumulate($1) a0
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
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_first::INTERVAL / a0._value_zeroth::BIGINT)::timestamp_interval) _return
	FROM
		_accumulate($1) a0
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
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_first::NUMERIC / a0._value_zeroth::BIGINT)::interval_numeric) _return
	FROM
		_accumulate($1) a0
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
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_first::INTERVAL / a0._value_zeroth::BIGINT)::interval_interval) _return
	FROM
		_accumulate($1) a0
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
$body$;