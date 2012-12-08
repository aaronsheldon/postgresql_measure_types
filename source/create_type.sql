----------------------------
-- Decimal measure spaces --
----------------------------

-- Decimal measures of decimal partitions
CREATE TYPE numeric_numeric AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage NUMERIC,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image NUMERIC
);

-- Character measures of decimal partitions
CREATE TYPE numeric_varchar AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage NUMERIC,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image CHARACTER VARYING
);

-- Timestamp measures of decimal partitions
CREATE TYPE numeric_timestamp AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage NUMERIC,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image TIMESTAMP
);

-- Interval measures of decimal partitions
CREATE TYPE numeric_interval AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage NUMERIC,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image INTERVAL
);

---------------------------
-- String measure spaces --
---------------------------

-- DEcimal measures of string partitions
CREATE TYPE varchar_numeric AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage CHARACTER VARYING,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image NUMERIC
);

-- Character measures of string partitions
CREATE TYPE varchar_varchar AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage CHARACTER VARYING,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image CHARACTER VARYING
);

-- Timestamp measures of string partitions
CREATE TYPE varchar_timestamp AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage CHARACTER VARYING,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image TIMESTAMP
);

-- Interval measures of string partitions
CREATE TYPE varchar_interval AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage CHARACTER VARYING,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image INTERVAL
);

------------------------------
-- Timestamp measure spaces --
------------------------------

-- Decimal measures of timestamp partitions
CREATE TYPE timestamp_numeric AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage TIMESTAMP,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image NUMERIC
);

-- Character measures of timestamp partitions
CREATE TYPE timestamp_varchar AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage TIMESTAMP,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image CHARACTER VARYING
);

-- Timestamp measures of timestamp partitions
CREATE TYPE timestamp_timestamp AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage TIMESTAMP,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image TIMESTAMP
);

-- Interval measures of timestamp partitions
CREATE TYPE timestamp_interval AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage TIMESTAMP,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image INTERVAL
);

------------------------------
-- Interval measure spaces --
------------------------------

-- Decimal measures of interval partitions
CREATE TYPE interval_numeric AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage INTERVAL,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image NUMERIC
);

-- Character measures of interval partitions
CREATE TYPE interval_varchar AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage INTERVAL,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image CHARACTER VARYING
);

-- Timestamp measures of interval partitions
CREATE TYPE interval_timestamp AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage INTERVAL,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image TIMESTAMP
);

-- Interval measures of interval partitions
CREATE TYPE interval_interval AS
(
	_key_infinite BOOLEAN,
	_key_finite BOOLEAN,
	_key_preimage INTERVAL,
	_key_topology BOOLEAN,
	_key_operation BOOLEAN,
	_key_ordinal BIGINT,
	_value_image INTERVAL
);