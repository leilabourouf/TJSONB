/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2016-2025, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * MobilityDB includes portions of PostGIS version 3 source code released
 * under the GNU General Public License (GPLv2 or later).
 * Copyright (c) 2001-2025, PostGIS contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

/**
 * @file
 * @brief R-tree GiST and SP-GiST indexes for temporal circular buffers
 */

/******************************************************************************/

CREATE FUNCTION tjsonb_gist_consistent(internal, tjsonb, smallint, oid, internal)
  RETURNS bool
  AS 'MODULE_PATHNAME', 'Span_gist_consistent'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

/******************************************************************************/

CREATE OPERATOR CLASS tjsonb_rtree_ops
  DEFAULT FOR TYPE tjsonb USING gist AS
  STORAGE tstzspan,
  -- strictly left
  OPERATOR  1    << (tjsonb, tstzspan),
  OPERATOR  1    << (tjsonb, tjsonb),
  -- overlaps or left
  OPERATOR  2    &< (tjsonb, tstzspan),
  OPERATOR  2    &< (tjsonb, tjsonb),
  -- overlaps
  OPERATOR  3    && (tjsonb, tstzspan),
  OPERATOR  3    && (tjsonb, tstzspan),
  OPERATOR  3    && (tjsonb, tjsonb),
  -- overlaps or right
  OPERATOR  4    &> (tjsonb, tstzspan),
  OPERATOR  4    &> (tjsonb, tjsonb),
    -- strictly right
  OPERATOR  5    >> (tjsonb, tstzspan),
  OPERATOR  5    >> (tjsonb, tjsonb),
    -- same
  OPERATOR  6    ~= (tjsonb, tstzspan),
  OPERATOR  6    ~= (tjsonb, tstzspan),
  OPERATOR  6    ~= (tjsonb, tjsonb),
  -- contains
  OPERATOR  7    @> (tjsonb, tstzspan),
  OPERATOR  7    @> (tjsonb, tstzspan),
  OPERATOR  7    @> (tjsonb, tjsonb),
  -- contained by
  OPERATOR  8    <@ (tjsonb, tstzspan),
  OPERATOR  8    <@ (tjsonb, tstzspan),
  OPERATOR  8    <@ (tjsonb, tjsonb),
  -- overlaps or below
  OPERATOR  9    &<| (tjsonb, tstzspan),
  OPERATOR  9    &<| (tjsonb, tjsonb),
  -- strictly below
  OPERATOR  10    <<| (tjsonb, tstzspan),
  OPERATOR  10    <<| (tjsonb, tjsonb),
  -- strictly above
  OPERATOR  11    |>> (tjsonb, tstzspan),
  OPERATOR  11    |>> (tjsonb, tjsonb),
  -- overlaps or above
  OPERATOR  12    |&> (tjsonb, tstzspan),
  OPERATOR  12    |&> (tjsonb, tjsonb),
  -- adjacent
  OPERATOR  17    -|- (tjsonb, tstzspan),
  OPERATOR  17    -|- (tjsonb, tstzspan),
  OPERATOR  17    -|- (tjsonb, tjsonb),
  -- nearest approach distance
--  OPERATOR  25    |=| (tjsonb, tstzspan) FOR ORDER BY pg_catalog.float_ops,
  -- overlaps or before
  OPERATOR  28    &<# (tjsonb, tstzspan),
  OPERATOR  28    &<# (tjsonb, tstzspan),
  OPERATOR  28    &<# (tjsonb, tjsonb),
  -- strictly before
  OPERATOR  29    <<# (tjsonb, tstzspan),
  OPERATOR  29    <<# (tjsonb, tstzspan),
  OPERATOR  29    <<# (tjsonb, tjsonb),
  -- strictly after
  OPERATOR  30    #>> (tjsonb, tstzspan),
  OPERATOR  30    #>> (tjsonb, tstzspan),
  OPERATOR  30    #>> (tjsonb, tjsonb),
  -- overlaps or after
  OPERATOR  31    #&> (tjsonb, tstzspan),
  OPERATOR  31    #&> (tjsonb, tstzspan),
  OPERATOR  31    #&> (tjsonb, tjsonb),
  -- functions
  FUNCTION  1 tjsonb_gist_consistent(internal, tjsonb, smallint, oid, internal),
  FUNCTION  2 tstzspan_gist_union(internal, internal),
  FUNCTION  3 tspatial_gist_compress(internal),
  FUNCTION  5 tstzspan_gist_penalty(internal, internal, internal),
  FUNCTION  6 tstzspan_gist_picksplit(internal, internal),
  FUNCTION  7 tstzspan_gist_same(tstzspan, tstzspan, internal);
--  FUNCTION  8 gist_tjsonb_distance(internal, tjsonb, smallint, oid, internal),

/******************************************************************************/

CREATE OPERATOR CLASS tjsonb_quadtree_ops
  DEFAULT FOR TYPE tjsonb USING spgist AS
  -- strictly left
  OPERATOR  1    << (tjsonb, tstzspan),
  OPERATOR  1    << (tjsonb, tjsonb),
  -- overlaps or left
  OPERATOR  2    &< (tjsonb, tstzspan),
  OPERATOR  2    &< (tjsonb, tjsonb),
  -- overlaps
  OPERATOR  3    && (tjsonb, tstzspan),
  OPERATOR  3    && (tjsonb, tstzspan),
  OPERATOR  3    && (tjsonb, tjsonb),
  -- overlaps or right
  OPERATOR  4    &> (tjsonb, tstzspan),
  OPERATOR  4    &> (tjsonb, tjsonb),
    -- strictly right
  OPERATOR  5    >> (tjsonb, tstzspan),
  OPERATOR  5    >> (tjsonb, tjsonb),
    -- same
  OPERATOR  6    ~= (tjsonb, tstzspan),
  OPERATOR  6    ~= (tjsonb, tstzspan),
  OPERATOR  6    ~= (tjsonb, tjsonb),
  -- contains
  OPERATOR  7    @> (tjsonb, tstzspan),
  OPERATOR  7    @> (tjsonb, tstzspan),
  OPERATOR  7    @> (tjsonb, tjsonb),
  -- contained by
  OPERATOR  8    <@ (tjsonb, tstzspan),
  OPERATOR  8    <@ (tjsonb, tstzspan),
  OPERATOR  8    <@ (tjsonb, tjsonb),
  -- overlaps or below
  OPERATOR  9    &<| (tjsonb, tstzspan),
  OPERATOR  9    &<| (tjsonb, tjsonb),
  -- strictly below
  OPERATOR  10    <<| (tjsonb, tstzspan),
  OPERATOR  10    <<| (tjsonb, tjsonb),
  -- strictly above
  OPERATOR  11    |>> (tjsonb, tstzspan),
  OPERATOR  11    |>> (tjsonb, tjsonb),
  -- overlaps or above
  OPERATOR  12    |&> (tjsonb, tstzspan),
  OPERATOR  12    |&> (tjsonb, tjsonb),
  -- adjacent
  OPERATOR  17    -|- (tjsonb, tstzspan),
  OPERATOR  17    -|- (tjsonb, tstzspan),
  OPERATOR  17    -|- (tjsonb, tjsonb),
  -- nearest approach distance
--  OPERATOR  25    |=| (tjsonb, tstzspan) FOR ORDER BY pg_catalog.float_ops,
  -- overlaps or before
  OPERATOR  28    &<# (tjsonb, tstzspan),
  OPERATOR  28    &<# (tjsonb, tstzspan),
  OPERATOR  28    &<# (tjsonb, tjsonb),
  -- strictly before
  OPERATOR  29    <<# (tjsonb, tstzspan),
  OPERATOR  29    <<# (tjsonb, tstzspan),
  OPERATOR  29    <<# (tjsonb, tjsonb),
  -- strictly after
  OPERATOR  30    #>> (tjsonb, tstzspan),
  OPERATOR  30    #>> (tjsonb, tstzspan),
  OPERATOR  30    #>> (tjsonb, tjsonb),
  -- overlaps or after
  OPERATOR  31    #&> (tjsonb, tstzspan),
  OPERATOR  31    #&> (tjsonb, tstzspan),
  OPERATOR  31    #&> (tjsonb, tjsonb),
  -- functions
  FUNCTION  1 tstzspan_spgist_config(internal, internal),
  FUNCTION  2 tstzspan_quadtree_choose(internal, internal),
  FUNCTION  3 tstzspan_quadtree_picksplit(internal, internal),
  FUNCTION  4 tstzspan_quadtree_inner_consistent(internal, internal),
  FUNCTION  5 tstzspan_spgist_leaf_consistent(internal, internal),
  FUNCTION  6 tspatial_spgist_compress(internal);

/******************************************************************************/

CREATE OPERATOR CLASS tjsonb_kdtree_ops
  FOR TYPE tjsonb USING spgist AS
  -- strictly left
  OPERATOR  1    << (tjsonb, tstzspan),
  OPERATOR  1    << (tjsonb, tjsonb),
  -- overlaps or left
  OPERATOR  2    &< (tjsonb, tstzspan),
  OPERATOR  2    &< (tjsonb, tjsonb),
  -- overlaps
  OPERATOR  3    && (tjsonb, tstzspan),
  OPERATOR  3    && (tjsonb, tstzspan),
  OPERATOR  3    && (tjsonb, tjsonb),
  -- overlaps or right
  OPERATOR  4    &> (tjsonb, tstzspan),
  OPERATOR  4    &> (tjsonb, tjsonb),
    -- strictly right
  OPERATOR  5    >> (tjsonb, tstzspan),
  OPERATOR  5    >> (tjsonb, tjsonb),
    -- same
  OPERATOR  6    ~= (tjsonb, tstzspan),
  OPERATOR  6    ~= (tjsonb, tstzspan),
  OPERATOR  6    ~= (tjsonb, tjsonb),
  -- contains
  OPERATOR  7    @> (tjsonb, tstzspan),
  OPERATOR  7    @> (tjsonb, tstzspan),
  OPERATOR  7    @> (tjsonb, tjsonb),
  -- contained by
  OPERATOR  8    <@ (tjsonb, tstzspan),
  OPERATOR  8    <@ (tjsonb, tstzspan),
  OPERATOR  8    <@ (tjsonb, tjsonb),
  -- overlaps or below
  OPERATOR  9    &<| (tjsonb, tstzspan),
  OPERATOR  9    &<| (tjsonb, tjsonb),
  -- strictly below
  OPERATOR  10    <<| (tjsonb, tstzspan),
  OPERATOR  10    <<| (tjsonb, tjsonb),
  -- strictly above
  OPERATOR  11    |>> (tjsonb, tstzspan),
  OPERATOR  11    |>> (tjsonb, tjsonb),
  -- overlaps or above
  OPERATOR  12    |&> (tjsonb, tstzspan),
  OPERATOR  12    |&> (tjsonb, tjsonb),
  -- adjacent
  OPERATOR  17    -|- (tjsonb, tstzspan),
  OPERATOR  17    -|- (tjsonb, tstzspan),
  OPERATOR  17    -|- (tjsonb, tjsonb),
  -- nearest approach distance
--  OPERATOR  25    |=| (tjsonb, tstzspan) FOR ORDER BY pg_catalog.float_ops,
  -- overlaps or before
  OPERATOR  28    &<# (tjsonb, tstzspan),
  OPERATOR  28    &<# (tjsonb, tstzspan),
  OPERATOR  28    &<# (tjsonb, tjsonb),
  -- strictly before
  OPERATOR  29    <<# (tjsonb, tstzspan),
  OPERATOR  29    <<# (tjsonb, tstzspan),
  OPERATOR  29    <<# (tjsonb, tjsonb),
  -- strictly after
  OPERATOR  30    #>> (tjsonb, tstzspan),
  OPERATOR  30    #>> (tjsonb, tstzspan),
  OPERATOR  30    #>> (tjsonb, tjsonb),
  -- overlaps or after
  OPERATOR  31    #&> (tjsonb, tstzspan),
  OPERATOR  31    #&> (tjsonb, tstzspan),
  OPERATOR  31    #&> (tjsonb, tjsonb),
  -- functions
  FUNCTION  1 tstzspan_spgist_config(internal, internal),
  FUNCTION  2 tstzspan_kdtree_choose(internal, internal),
  FUNCTION  3 tstzspan_kdtree_picksplit(internal, internal),
  FUNCTION  4 tstzspan_kdtree_inner_consistent(internal, internal),
  FUNCTION  5 tstzspan_spgist_leaf_consistent(internal, internal),
  FUNCTION  6 tspatial_spgist_compress(internal);

/******************************************************************************/
