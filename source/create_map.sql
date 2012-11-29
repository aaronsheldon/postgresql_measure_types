----------------------------
-- Generic measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _map(_measure anyarray) RETURNS SETOF RECORD LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Inverse Constructor to the Measure and Indicator Constructors
 *
 * Generic polymorphic implementation coercing a representation 
 * of a simple measurable function into a table containing columns
 *
 *   - topology of the lower bound
 *   - preimage lower bound
 *   - preimage upper bound
 *   - topology of the upper bound
 *   - image of the interval  
 *
 * @author Aaron Sheldon
 * @param array Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT 
		CASE
			WHEN ($1[a0._index])._key_topology THEN
				'('
			ELSE
				'['
		END _lower_topology, 
		($1[a0._index])._key_preimage _lower_preimage,
		($1[a0._index + 1])._key_preimage _upper_preimage,
		CASE
			WHEN ($1[a0._index + 1])._key_topology THEN
				']'
			ELSE
				')'
		END _upper_topology,  
		($1[a0._index])._value_image _interval_image 
	FROM 
		generate_series(1, array_length($1, 1) - 1, 2) a0(_index);
$body$;

----------------------------
-- Decimal measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION map(_measure numeric_numeric[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING,
	lower_preimage NUMERIC,
	upper_preimage NUMERIC, 
	upper_topology CHARACTER VARYING,
	interval_image NUMERIC
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param numeric_numeric Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure numeric_varchar[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage NUMERIC,
	upper_preimage NUMERIC, 
	upper_topology CHARACTER VARYING,
	interval_image CHARACTER VARYING
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param numeric_varchar Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure numeric_timestamp[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage NUMERIC,
	upper_preimage NUMERIC, 
	upper_topology CHARACTER VARYING,
	interval_image TIMESTAMP
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param numeric_timestamp Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure numeric_interval[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage NUMERIC,
	upper_preimage NUMERIC, 
	upper_topology CHARACTER VARYING,
	interval_image INTERVAL
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param numeric_interval Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

--------------------------
-- Sting measure spaces --
--------------------------

CREATE OR REPLACE FUNCTION map(_measure varchar_numeric[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage CHARACTER VARYING,
	upper_preimage CHARACTER VARYING, 
	upper_topology CHARACTER VARYING,
	interval_image NUMERIC
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param varchar_numeric Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure varchar_varchar[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage CHARACTER VARYING,
	upper_preimage CHARACTER VARYING, 
	upper_topology CHARACTER VARYING,
	interval_image CHARACTER VARYING
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param varchar_varchar Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure varchar_timestamp[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage CHARACTER VARYING,
	upper_preimage CHARACTER VARYING, 
	upper_topology CHARACTER VARYING,
	interval_image TIMESTAMP
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param varchar_timestamp Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure varchar_interval[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage CHARACTER VARYING,
	upper_preimage CHARACTER VARYING, 
	upper_topology CHARACTER VARYING,
	interval_image INTERVAL
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param varchar_interval Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

-------------------------
-- Time measure spaces --
-------------------------

CREATE OR REPLACE FUNCTION map(_measure timestamp_numeric[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage TIMESTAMP,
	upper_preimage TIMESTAMP, 
	upper_topology CHARACTER VARYING,
	interval_image NUMERIC
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param timestamp_numeric Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure timestamp_varchar[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage TIMESTAMP,
	upper_preimage TIMESTAMP, 
	upper_topology CHARACTER VARYING,
	interval_image CHARACTER VARYING
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param timestamp_varchar Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure timestamp_timestamp[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage TIMESTAMP,
	upper_preimage TIMESTAMP, 
	upper_topology CHARACTER VARYING,
	interval_image TIMESTAMP
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param timestamp_timestamp Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure timestamp_interval[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage TIMESTAMP,
	upper_preimage TIMESTAMP, 
	upper_topology CHARACTER VARYING,
	interval_image INTERVAL
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param timestamp_interval Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

-----------------------------
-- Interval measure spaces --
-----------------------------

CREATE OR REPLACE FUNCTION map(_measure interval_numeric[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage INTERVAL,
	upper_preimage INTERVAL, 
	upper_topology CHARACTER VARYING,
	interval_image NUMERIC
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param interval_numeric Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure interval_varchar[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage INTERVAL,
	upper_preimage INTERVAL, 
	upper_topology CHARACTER VARYING,
	interval_image CHARACTER VARYING
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param interval_varchar Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure interval_timestamp[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage INTERVAL,
	upper_preimage INTERVAL, 
	upper_topology CHARACTER VARYING,
	interval_image TIMESTAMP
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param interval_timestamp Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;

CREATE OR REPLACE FUNCTION map(_measure interval_interval[]) RETURNS TABLE
( 
	lower_topology CHARACTER VARYING, 
	lower_preimage INTERVAL,
	upper_preimage INTERVAL, 
	upper_topology CHARACTER VARYING,
	interval_image INTERVAL
) 
LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Wrapper function to coerce type 
 *
 * Given a table coerce rows to the matching type
 *
 * @author Aaron Sheldon
 * @param interval_interval Simple measurable function
 * @return table Representation of the mapping of a simple measurable function
 */
	SELECT _map($1) _return;
$body$;