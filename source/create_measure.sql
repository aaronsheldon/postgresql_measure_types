----------------------------
-- Generic Measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage anyelement,
	_upper_preimage anyelement,
	_upper_topology CHARACTER VARYING,
	_interval_image NUMERIC
)
RETURNS TABLE
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage anyelement,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_value_image NUMERIC
)
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Partially Generic Polymorphic Interval Measure Constructor
 *
 * Instantiate a representation of a measure function
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
 * @param number image of the interval
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
			(1, FALSE, FALSE, NULL, TRUE, TRUE, $5),
			(1, TRUE, FALSE, NULL, FALSE, FALSE, $5),
			(2, FALSE, FALSE, NULL, TRUE, TRUE, $5),
			(2, FALSE, TRUE, $3, $4 = ']', FALSE, $5),
			(2, FALSE, TRUE, $3, $4 = ']', TRUE, NULL),
			(2, TRUE, FALSE, NULL, FALSE, FALSE, NULL),
			(3, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(3, FALSE, TRUE, $2, $1 = '(', FALSE, NULL),
			(3, FALSE, TRUE, $2, $1 = '(', TRUE, $5),
			(3, TRUE, FALSE, NULL, FALSE, FALSE, $5),
			(4, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(4, FALSE, TRUE, $2, FALSE, FALSE, NULL),
			(4, FALSE, TRUE, $2, FALSE, TRUE, $5),
			(4, FALSE, TRUE, $3, TRUE, FALSE, $5),
			(4, FALSE, TRUE, $3, TRUE, TRUE, NULL),
			(4, TRUE, FALSE, NULL, FALSE, FALSE, NULL),
			(5, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(5, FALSE, TRUE, $2, $1 = '(', FALSE, NULL),
			(5, FALSE, TRUE, $2, $1 = '(', TRUE, $5),
			(5, FALSE, TRUE, $3, $4 = ']', FALSE, $5),
			(5, FALSE, TRUE, $3, $4 = ']', TRUE, NULL),
			(5, TRUE, FALSE, NULL, FALSE, FALSE, NULL)
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

CREATE OR REPLACE FUNCTION _measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage anyelement,
	_upper_preimage anyelement,
	_upper_topology CHARACTER VARYING,
	_interval_image CHARACTER VARYING
)
RETURNS TABLE
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage anyelement,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_value_image CHARACTER VARYING
)
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Generic Polymorphic Interval Measure Constructor
 *
 * Instantiate a representation of a measure function
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
 * @param string image of the interval
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
		a0._value_image::CHARACTER VARYING
	FROM
		(
			VALUES
			(1, FALSE, FALSE, NULL, TRUE, TRUE, $5),
			(1, TRUE, FALSE, NULL, FALSE, FALSE, $5),
			(2, FALSE, FALSE, NULL, TRUE, TRUE, $5),
			(2, FALSE, TRUE, $3, $4 = ']', FALSE, $5),
			(2, FALSE, TRUE, $3, $4 = ']', TRUE, NULL),
			(2, TRUE, FALSE, NULL, FALSE, FALSE, NULL),
			(3, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(3, FALSE, TRUE, $2, $1 = '(', FALSE, NULL),
			(3, FALSE, TRUE, $2, $1 = '(', TRUE, $5),
			(3, TRUE, FALSE, NULL, FALSE, FALSE, $5),
			(4, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(4, FALSE, TRUE, $2, FALSE, FALSE, NULL),
			(4, FALSE, TRUE, $2, FALSE, TRUE, $5),
			(4, FALSE, TRUE, $3, TRUE, FALSE, $5),
			(4, FALSE, TRUE, $3, TRUE, TRUE, NULL),
			(4, TRUE, FALSE, NULL, FALSE, FALSE, NULL),
			(5, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(5, FALSE, TRUE, $2, $1 = '(', FALSE, NULL),
			(5, FALSE, TRUE, $2, $1 = '(', TRUE, $5),
			(5, FALSE, TRUE, $3, $4 = ']', FALSE, $5),
			(5, FALSE, TRUE, $3, $4 = ']', TRUE, NULL),
			(5, TRUE, FALSE, NULL, FALSE, FALSE, NULL)
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

CREATE OR REPLACE FUNCTION _measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage anyelement,
	_upper_preimage anyelement,
	_upper_topology CHARACTER VARYING,
	_interval_image TIMESTAMP
)
RETURNS TABLE
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage anyelement,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_value_image TIMESTAMP
)
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Generic Polymorphic Interval Measure Constructor
 *
 * Instantiate a representation of a measure function
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
 * @param timestamp image of the interval
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
		a0._value_image::TIMESTAMP
	FROM
		(
			VALUES
			(1, FALSE, FALSE, NULL, TRUE, TRUE, $5),
			(1, TRUE, FALSE, NULL, FALSE, FALSE, $5),
			(2, FALSE, FALSE, NULL, TRUE, TRUE, $5),
			(2, FALSE, TRUE, $3, $4 = ']', FALSE, $5),
			(2, FALSE, TRUE, $3, $4 = ']', TRUE, NULL),
			(2, TRUE, FALSE, NULL, FALSE, FALSE, NULL),
			(3, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(3, FALSE, TRUE, $2, $1 = '(', FALSE, NULL),
			(3, FALSE, TRUE, $2, $1 = '(', TRUE, $5),
			(3, TRUE, FALSE, NULL, FALSE, FALSE, $5),
			(4, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(4, FALSE, TRUE, $2, FALSE, FALSE, NULL),
			(4, FALSE, TRUE, $2, FALSE, TRUE, $5),
			(4, FALSE, TRUE, $3, TRUE, FALSE, $5),
			(4, FALSE, TRUE, $3, TRUE, TRUE, NULL),
			(4, TRUE, FALSE, NULL, FALSE, FALSE, NULL),
			(5, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(5, FALSE, TRUE, $2, $1 = '(', FALSE, NULL),
			(5, FALSE, TRUE, $2, $1 = '(', TRUE, $5),
			(5, FALSE, TRUE, $3, $4 = ']', FALSE, $5),
			(5, FALSE, TRUE, $3, $4 = ']', TRUE, NULL),
			(5, TRUE, FALSE, NULL, FALSE, FALSE, NULL)
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

CREATE OR REPLACE FUNCTION _measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage anyelement,
	_upper_preimage anyelement,
	_upper_topology CHARACTER VARYING,
	_interval_image INTERVAL
)
RETURNS TABLE
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage anyelement,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_value_image INTERVAL
)
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Generic Polymorphic Interval Measure Constructor
 *
 * Instantiate a representation of a measure function
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
 * @param timespan image of the interval
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
		a0._value_image::INTERVAL
	FROM
		(
			VALUES
			(1, FALSE, FALSE, NULL, TRUE, TRUE, $5),
			(1, TRUE, FALSE, NULL, FALSE, FALSE, $5),
			(2, FALSE, FALSE, NULL, TRUE, TRUE, $5),
			(2, FALSE, TRUE, $3, $4 = ']', FALSE, $5),
			(2, FALSE, TRUE, $3, $4 = ']', TRUE, NULL),
			(2, TRUE, FALSE, NULL, FALSE, FALSE, NULL),
			(3, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(3, FALSE, TRUE, $2, $1 = '(', FALSE, NULL),
			(3, FALSE, TRUE, $2, $1 = '(', TRUE, $5),
			(3, TRUE, FALSE, NULL, FALSE, FALSE, $5),
			(4, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(4, FALSE, TRUE, $2, FALSE, FALSE, NULL),
			(4, FALSE, TRUE, $2, FALSE, TRUE, $5),
			(4, FALSE, TRUE, $3, TRUE, FALSE, $5),
			(4, FALSE, TRUE, $3, TRUE, TRUE, NULL),
			(4, TRUE, FALSE, NULL, FALSE, FALSE, NULL),
			(5, FALSE, FALSE, NULL, TRUE, TRUE, NULL),
			(5, FALSE, TRUE, $2, $1 = '(', FALSE, NULL),
			(5, FALSE, TRUE, $2, $1 = '(', TRUE, $5),
			(5, FALSE, TRUE, $3, $4 = ']', FALSE, $5),
			(5, FALSE, TRUE, $3, $4 = ']', TRUE, NULL),
			(5, TRUE, FALSE, NULL, FALSE, FALSE, NULL)
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

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage NUMERIC,
	_upper_preimage NUMERIC,
	_upper_topology CHARACTER VARYING,
	_interval_image NUMERIC
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
 * @param number image of the interval
 * @return numeric_numeric Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_numeric) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage NUMERIC,
	_upper_preimage NUMERIC,
	_upper_topology CHARACTER VARYING,
	_interval_image CHARACTER VARYING
)
RETURNS numeric_varchar[] LANGUAGE sql IMMUTABLE AS
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
 * @param string image of the interval
 * @return numeric_varchar Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_varchar) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage NUMERIC,
	_upper_preimage NUMERIC,
	_upper_topology CHARACTER VARYING,
	_interval_image TIMESTAMP
)
RETURNS numeric_timestamp[] LANGUAGE sql IMMUTABLE AS
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
 * @param timestamp image of the interval
 * @return numeric_timestamp Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_timestamp) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage NUMERIC,
	_upper_preimage NUMERIC,
	_upper_topology CHARACTER VARYING,
	_interval_image INTERVAL
)
RETURNS numeric_interval[] LANGUAGE sql IMMUTABLE AS
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
 * @param timespan image of the interval
 * @return numeric_interval Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::numeric_interval) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

---------------------------
-- String Measure spaces --
---------------------------

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage CHARACTER VARYING,
	_upper_preimage CHARACTER VARYING,
	_upper_topology CHARACTER VARYING,
	_interval_image NUMERIC
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
 * @param number image of the interval
 * @return varchar_numeric Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_numeric) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage CHARACTER VARYING,
	_upper_preimage CHARACTER VARYING,
	_upper_topology CHARACTER VARYING,
	_interval_image CHARACTER VARYING
)
RETURNS varchar_varchar[] LANGUAGE sql IMMUTABLE AS
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
 * @param string image of the interval
 * @return varchar_varchar Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_varchar) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage CHARACTER VARYING,
	_upper_preimage CHARACTER VARYING,
	_upper_topology CHARACTER VARYING,
	_interval_image TIMESTAMP
)
RETURNS varchar_timestamp[] LANGUAGE sql IMMUTABLE AS
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
 * @param timestamp image of the interval
 * @return varchar_timestamp Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_timestamp) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage CHARACTER VARYING,
	_upper_preimage CHARACTER VARYING,
	_upper_topology CHARACTER VARYING,
	_interval_image INTERVAL
)
RETURNS varchar_interval[] LANGUAGE sql IMMUTABLE AS
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
 * @param timespan image of the interval
 * @return varchar_interval Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::varchar_interval) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

-------------------------
-- Time Measure spaces --
-------------------------

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage TIMESTAMP,
	_upper_preimage TIMESTAMP,
	_upper_topology CHARACTER VARYING,
	_interval_image NUMERIC
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
 * @param timestamp lower bound of the interval
 * @param timestamp upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @param number image of the interval
 * @return timestamp_numeric Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_numeric) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage TIMESTAMP,
	_upper_preimage TIMESTAMP,
	_upper_topology CHARACTER VARYING,
	_interval_image CHARACTER VARYING
)
RETURNS timestamp_varchar[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param timestamp lower bound of the interval
 * @param timestamp upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @param string image of the interval
 * @return timestamp_varchar Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_varchar) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage TIMESTAMP,
	_upper_preimage TIMESTAMP,
	_upper_topology CHARACTER VARYING,
	_interval_image TIMESTAMP
)
RETURNS timestamp_timestamp[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param timestamp lower bound of the interval
 * @param timestamp upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @param timestamp image of the interval
 * @return timestamp_timestamp Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_timestamp) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage TIMESTAMP,
	_upper_preimage TIMESTAMP,
	_upper_topology CHARACTER VARYING,
	_interval_image INTERVAL
)
RETURNS timestamp_interval[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param timestamp lower bound of the interval
 * @param timestamp upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @param timespan image of the interval
 * @return timestamp_interval Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::timestamp_interval) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

-----------------------------
-- Interval Measure spaces --
-----------------------------

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage INTERVAL,
	_upper_preimage INTERVAL,
	_upper_topology CHARACTER VARYING,
	_interval_image NUMERIC
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
 * @param timespan lower bound of the interval
 * @param timespan upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @param number image of the interval
 * @return interval_numeric Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_numeric) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage INTERVAL,
	_upper_preimage INTERVAL,
	_upper_topology CHARACTER VARYING,
	_interval_image CHARACTER VARYING
)
RETURNS interval_varchar[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param timespan lower bound of the interval
 * @param timespan upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @param string image of the interval
 * @return interval_varchar Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_varchar) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage INTERVAL,
	_upper_preimage INTERVAL,
	_upper_topology CHARACTER VARYING,
	_interval_image TIMESTAMP
)
RETURNS interval_timestamp[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param timespan lower bound of the interval
 * @param timespan upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @param timestamp image of the interval
 * @return interval_timestamp Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_timestamp) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;

CREATE OR REPLACE FUNCTION measure
(
	_lower_topology CHARACTER VARYING,
	_lower_preimage INTERVAL,
	_upper_preimage INTERVAL,
	_upper_topology CHARACTER VARYING,
	_interval_image INTERVAL
)
RETURNS interval_interval[] LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param string topology of the lower bound of the interval
 * @param timespan lower bound of the interval
 * @param timespan upper bound of the interval
 * @param string topology of the upper bound of the interval
 * @param timespan image of the interval
 * @return interval_interval Representation of a simple measurable function
 */
	SELECT
		array_agg(ROW(a0._key_infinite, a0._key_finite, a0._key_preimage, a0._key_topology, a0._key_operation, a0._value_image)::interval_interval) _return
	FROM
		_measure($1, $2, $3, $4, $5) a0;
$body$;