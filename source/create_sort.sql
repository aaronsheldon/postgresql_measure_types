----------------------------
-- Generic measure spaces --
----------------------------

CREATE OR REPLACE FUNCTION _sort(_measure anyarray) RETURNS TABLE (_key_index BIGINT) LANGUAGE sql IMMUTABLE AS
$body$
/*
 * Sort Array Subscripts in Topological Order
 *
 * Given a list of the boundaries of an arbitrary set of simple measurable functions, sort
 * the boundaries in order of the value of the images, and then in correct topological order
 * of the preimage of the boundaries, respecting the implicit ordering of the stack of functions. 
 *
 * @author Aaron Sheldon
 * @param array Stack of simple measurable function
 * @return table Subscripts
 */
	SELECT 
		a0._key_index::BIGINT
	FROM
		generate_series(1, array_length($1, 1), 1) a0(_key_index)
	ORDER BY
		($1[a0._key_index])._key_infinite ASC NULLS FIRST,
		($1[a0._key_index])._key_finite ASC NULLS FIRST,
		($1[a0._key_index])._key_preimage ASC NULLS FIRST,
		($1[a0._key_index])._key_topology ASC NULLS FIRST,
		($1[a0._key_index])._key_operation ASC NULLS FIRST,
		a0._key_index ASC NULLS FIRST;
$body$;