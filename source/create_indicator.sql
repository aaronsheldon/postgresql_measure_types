----------------------------
-- Generic Measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _indicator
(
	IN _lower_topology CHARACTER VARYING, 
	IN _lower_preimage anyelement, 
	IN _upper_preimage anyelement, 
	IN _upper_topology CHARACTER VARYING,
	OUT _key_infinite BOOLEAN,
	OUT _key_finite BOOLEAN,
	OUT _key_preimage anyelement,
	OUT _key_topology BOOLEAN,
	OUT _key_operation BOOLEAN,
	OUT _value_image NUMERIC
) 
RETURNS SETOF RECORD LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Generic Polymorphic Interval Indicator Constructor
 *
 * Instantiate a representation of an indicator function
 * of an interval. A simple interval implicitly defines a 
 * partition contiaing three intervals; the interval before 
 * the bounded interval, the bounded interval and the interval
 * after the bounded interval. The representation is an array
 * of composite key-value pairs:
 *
 *   - key attribute: is the preimage positive infinite
 *   - key attribute: is the preimage finite
 *   - key attribute: preimage
 *   - key attribute: is the preimage between right closed and left open sets
 *   - key attribute: is the image pushed to the stack
 *   - value attribute: image
 *
 * Uses a simple hash in the where clause to select the appropriate boundary
 * representation
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param polymorphic lower bound of the interval
 * @param polymorphic upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @return table Representation of a simple measurable function
 * @todo return error for incorrect boundaries instead of returning empty set
 * @todo refine type coercion, eliminate unnessecary casting
 */
	SELECT
		a0._key_infinite::BOOLEAN,
		a0._key_finite::BOOLEAN,
		a0._key_preimage,
		a0._key_topology::BOOLEAN,
		a0._key_operation::BOOLEAN,
		a0._value_image::NUMERIC
	FROM
		(
			VALUES
			(1, FALSE, FALSE, NULL, TRUE, TRUE, 1),
			(1, TRUE, FALSE, NULL, FALSE, FALSE, 1),
			(2, FALSE, FALSE, NULL, TRUE, TRUE, 1),
			(2, FALSE, TRUE, $3, $4 = ']', FALSE, 1),
			(2, FALSE, TRUE, $3, $4 = ']', TRUE, 0),
			(2, TRUE, FALSE, NULL, FALSE, FALSE, 0),
			(3, FALSE, FALSE, NULL, TRUE, TRUE, 0),
			(3, FALSE, TRUE, $2, $1 = '(', FALSE, 0),
			(3, FALSE, TRUE, $2, $1 = '(', TRUE, 1),
			(3, TRUE, FALSE, NULL, FALSE, FALSE, 1),
			(4, FALSE, FALSE, NULL, TRUE, TRUE, 0),
			(4, FALSE, TRUE, $2, FALSE, FALSE, 0),
			(4, FALSE, TRUE, $2, FALSE, TRUE, 1),
			(4, FALSE, TRUE, $3, TRUE, FALSE, 1),
			(4, FALSE, TRUE, $3, TRUE, TRUE, 0),
			(4, TRUE, FALSE, NULL, FALSE, FALSE, 0),
			(5, FALSE, FALSE, NULL, TRUE, TRUE, 0),
			(5, FALSE, TRUE, $2, $1 = '(', FALSE, 0),
			(5, FALSE, TRUE, $2, $1 = '(', TRUE, 1),
			(5, FALSE, TRUE, $3, $4 = ']', FALSE, 1),
			(5, FALSE, TRUE, $3, $4 = ']', TRUE, 0),
			(5, TRUE, FALSE, NULL, FALSE, FALSE, 0)
		) 
		a0
		(
			_key_selector, 
			_key_infinite, 
			_key_finite, 
			_key_preimage, 
			_key_topology, 
			_key_operation, 
			_value_image
		)
	WHERE
		CASE
			WHEN $1 = '(' AND $2 IS NULL AND $3 IS NULL AND $4 = ')' THEN
				a0._key_selector = 1
			WHEN $1 = '(' AND $2 IS NULL AND $3 IS NOT NULL AND $4 IN (')', ']') THEN
				a0._key_selector = 2
			WHEN $1 IN ('(', '[') AND $2 IS NOT NULL AND $3 IS NULL AND $4 = ')' THEN
				a0._key_selector = 3
			WHEN $1 = '[' AND $2 = $3 AND $4 = ']' THEN
				a0._key_selector = 4
			WHEN $1 IN ('(', '[') AND $2 < $3 AND $4 IN (')', ']') THEN
				a0._key_selector = 5
			ELSE
				FALSE
		END;
$body$;

----------------------------
-- Decimal Measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION indicator
(
	_lower_topology CHARACTER VARYING, 
	_lower_preimage NUMERIC, 
	_upper_preimage NUMERIC, 
	_upper_topology CHARACTER VARYING
) 
RETURNS numeric_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param number lower bound of the interval
 * @param number upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @return numeric_numeric Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_numeric) _return
	FROM
		_indicator($1, $2, $3, $4) a0;
$body$;

---------------------------
-- String measure spaces --
---------------------------

CREATE OR REPLACE FUNCTION indicator
(
	_lower_topology CHARACTER VARYING, 
	_lower_preimage CHARACTER VARYING, 
	_upper_preimage CHARACTER VARYING, 
	_upper_topology CHARACTER VARYING
) 
RETURNS varchar_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param string lower bound of the interval
 * @param string upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @return varchar_numeric Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_numeric) _return
	FROM
		_indicator($1, $2, $3, $4) a0;
$body$;

-------------------------
-- Time measure spaces --
-------------------------

CREATE OR REPLACE FUNCTION indicator
(
	_lower_topology CHARACTER VARYING, 
	_lower_preimage TIMESTAMP, 
	_upper_preimage TIMESTAMP, 
	_upper_topology CHARACTER VARYING
) 
RETURNS timestamp_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param time lower bound of the interval
 * @param time upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @return timestamp_numeric Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_numeric) _return
	FROM
		_indicator($1, $2, $3, $4) a0;;
$body$;

-----------------------------
-- Interval measure spaces --
-----------------------------

CREATE OR REPLACE FUNCTION indicator
(
	_lower_topology CHARACTER VARYING, 
	_lower_preimage INTERVAL, 
	_upper_preimage INTERVAL, 
	_upper_topology CHARACTER VARYING
) 
RETURNS interval_numeric[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param time lower bound of the interval
 * @param time upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @return interval_numeric Representation of a simple measurable function
 */
		SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_numeric) _return
	FROM
		_indicator($1, $2, $3, $4) a0;
$body$;