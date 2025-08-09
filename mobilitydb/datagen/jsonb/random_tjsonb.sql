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
 * documentation FOR any purjsonb, without fee, and without a written
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
 * AND FITNESS FOR A PARTICULAR PURjsonb. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

/**
 * @file
 * @brief Basic synthetic data generator functions for temporal JSONB types
 */

------------------------------------------------------------------------------
-- Static JSONB
------------------------------------------------------------------------------

/**
 * @brief Generate a random jsonb value
 * @param[in] lowId, highId Inclusive bounds of the range for the vehicleId
 * @param[in] lowtime, hightime Inclusive bounds of the range for the tstzspan
 * @param[in] lowx, highx Inclusive bounds of the range for the x coordinates
 * @param[in] lowy, highy Inclusive bounds of the range for the y coordinates
 * @param[in] srid Optional SRID for the point of the jsonb
 */
DROP FUNCTION IF EXISTS random_jsonb;
CREATE FUNCTION random_jsonb(lowId int, highId int, lowtime timestamptz, 
  hightime timestamptz, lowx float, highx float, lowy float, highy float,
  srid int DEFAULT 0)
  RETURNS jsonb AS $$
BEGIN
  RETURN jsonb_build_object('vehicleId', random_int(lowId, highId)::text,
  'location', jsonb_object(ARRAY[text 'geom', 'timestamp'], 
    ARRAY[random_geom_point(lowx, highx, lowy, highy, srid)::text,
      random_timestamptz(lowtime, hightime)::text])) AS jb;
END;
$$ LANGUAGE PLPGSQL STRICT;

/*
SELECT k, random_jsonb(1, 100, '2001-01-01', '2001-12-31', -100, 100, -100, 100) AS jb
FROM generate_series(1,10) k;

SELECT k, random_jsonb(1, 100, '2001-01-01', '2001-12-31', -100, 100, -100, 100, 5676) AS jb
FROM generate_series(1,10) k;
*/

-------------------------------------------------------------------------------

/**
 * @brief Generate an array of random jsonb values
 * @param[in] lowId, highId Inclusive bounds of the range for the vehicleId
 * @param[in] lowtime, hightime Inclusive bounds of the range for the tstzspan
 * @param[in] lowx, highx Inclusive bounds of the range for the x coordinates
 * @param[in] lowy, highy Inclusive bounds of the range for the y coordinates
 * @param[in] mincard, maxcard Inclusive bounds of the cardinality of the array
 * @param[in] srid Optional SRID for the point of the jsonb
 */
DROP FUNCTION IF EXISTS random_jsonb_array;
CREATE FUNCTION random_jsonb_array(lowId int, highId int, lowtime timestamptz, 
  hightime timestamptz, lowx float, highx float, lowy float, highy float,
  mincard int, maxcard int, srid int DEFAULT 0)
  RETURNS jsonb[] AS $$
DECLARE
  result jsonb[];
  card int;
BEGIN
  IF mincard > maxcard THEN
    RAISE EXCEPTION 'mincard must be less than or equal to maxcard: %, %',
      mincard, maxcard;
  END IF;
  card = random_int(mincard, maxcard);
  FOR i IN 1..card
  LOOP
    result[i] = random_jsonb(lowId, highId, lowtime, hightime, lowx, highx,
      lowy, highy, srid);
  END LOOP;
  RETURN result;
END;
$$ LANGUAGE PLPGSQL STRICT;

/*
SELECT k, random_jsonb_array(1, 100, '2001-01-01', '2001-12-31', -100, 100, -100, 100, 1, 10) AS jb
FROM generate_series(1, 15) AS k;

SELECT k, random_jsonb_array(1, 100, '2001-01-01', '2001-12-31', -100, 100, -100, 100, 1, 10, 5676) AS jb
FROM generate_series(1, 15) AS k;
*/

-------------------------------------------------------------------------------

/**
 * @brief Generate a set of random jsonb values
 * @param[in] lowId, highId Inclusive bounds of the range for the vehicleId
 * @param[in] lowtime, hightime Inclusive bounds of the range for the tstzspan
 * @param[in] lowx, highx Inclusive bounds of the range for the x coordinates
 * @param[in] lowy, highy Inclusive bounds of the range for the y coordinates
 * @param[in] mincard, maxcard Inclusive bounds of the cardinality of the set
 * @param[in] srid Optional SRID for the point of the jsonb
 */
DROP FUNCTION IF EXISTS random_jsonb_set;
CREATE FUNCTION random_jsonb_set(lowId int, highId int, lowtime timestamptz, 
  hightime timestamptz, lowx float, highx float, lowy float, highy float,
  mincard int, maxcard int, srid int DEFAULT 0)
  RETURNS jsonbset AS $$
DECLARE
  nparr jsonb[];
BEGIN
  RETURN set(random_jsonb_array(lowId, highId, lowtime, hightime, lowx, highx,
    lowy, highy, mincard, maxcard, srid));
END;
$$ LANGUAGE PLPGSQL STRICT;

/*
SELECT k, random_jsonb_set(1, 100, '2001-01-01', '2001-12-31', -100, 100, -100, 100, 1, 10) AS jb
FROM generate_series(1, 15) AS k;

SELECT k, random_jsonb_set(1, 100, '2001-01-01', '2001-12-31', -100, 100, -100, 100, 1, 10, 5676) AS jb
FROM generate_series(1, 15) AS k;
*/

------------------------------------------------------------------------------
-- Temporal JSONB
------------------------------------------------------------------------------

/**
 * @brief Generate a random temporal JSONB of instant subtype
 * @param[in] lowId, highId Inclusive bounds of the range for the vehicleId
 * @param[in] lowx, highx Inclusive bounds of the range for the x coordinates
 * @param[in] lowy, highy Inclusive bounds of the range for the y coordinates
 * @param[in] lowtime, hightime Inclusive bounds of the tstzspan
 * @param[in] srid Optional SRID for the point of the jsonb
 */
DROP FUNCTION IF EXISTS random_tjsonb_inst;
CREATE FUNCTION random_tjsonb_inst(lowId int, highId int, lowx float,
  highx float, lowy float, highy float, lowtime timestamptz,
  hightime timestamptz, srid int DEFAULT 0)
  RETURNS tjsonb AS $$
BEGIN
  RETURN tjsonb(random_jsonb(lowId, highId, lowtime, hightime, lowx, highx,
    lowy, highy, srid), random_timestamptz(lowtime, hightime));
END;
$$ LANGUAGE PLPGSQL STRICT;

/*
SELECT k, random_tjsonb_inst(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31') AS inst
FROM generate_series(1,10) k;
*/

-------------------------------------------------------------------------------

/**
 * @brief Generate a random temporal JSONB of discrete sequence subtype
 * @param[in] lowId, highId Inclusive bounds of the range for the vehicleId
 * @param[in] lowx, highx Inclusive bounds of the range for the x coordinates
 * @param[in] lowy, highy Inclusive bounds of the range for the y coordinates
 * @param[in] lowtime, hightime Inclusive bounds of the tstzspan
 * @param[in] maxminutes Maximum number of minutes between consecutive instants
 * @param[in] mincard, maxcard Inclusive bounds of the number of instants
 * @param[in] srid Optional SRID for the point of the jsonb
 */
DROP FUNCTION IF EXISTS random_tjsonb_discseq;
CREATE FUNCTION random_tjsonb_discseq(lowId int, highId int, lowx float,
  highx float, lowy float, highy float, lowtime timestamptz, 
  hightime timestamptz, maxminutes int, mincard int, maxcard int,
  srid int DEFAULT 0)
  RETURNS tjsonb AS $$
DECLARE
  result tjsonb[];
  card int;
  t timestamptz;
BEGIN
  card = random_int(1, maxcard);
  t = random_timestamptz(lowtime, hightime);
  FOR i IN 1..card
  LOOP
    result[i] = tjsonb(random_jsonb(lowId, highId, lowtime, hightime, lowx,
      highx, lowy, highy, srid), t);
    t = t + random_minutes(1, maxminutes);
  END LOOP;
  RETURN tjsonbSeq(result, 'Discrete');
END;
$$ LANGUAGE PLPGSQL STRICT;

/*
SELECT k, random_tjsonb_discseq(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 10, 1, 10) AS ti
FROM generate_series(1,10) k;

SELECT k, random_tjsonb_discseq(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 10, 1, 10, 5676) AS ti
FROM generate_series(1,10) k;
*/

-------------------------------------------------------------------------------

/**
 * @brief Generate a random temporal JSONB of sequence subtype
 * @param[in] lowId, highId Inclusive bounds of the range for the vehicleId
 * @param[in] lowx, highx Inclusive bounds of the range for the x coordinates
 * @param[in] lowy, highy Inclusive bounds of the range for the y coordinates
 * @param[in] lowtime, hightime Inclusive bounds of the tstzspan
 * @param[in] maxminutes Maximum number of minutes between consecutive instants
 * @param[in] mincard, maxcard Inclusive bounds of the number of instants
 * @param[in] srid Optional SRID for the point of the jsonb
 * @param[in] fixstart True when this function is called for generating a
 *    sequence set value and in this case the start timestamp is already fixed
 */
DROP FUNCTION IF EXISTS random_tjsonb_contseq;
CREATE FUNCTION random_tjsonb_contseq(lowId int, highId int, lowx float,
  highx float, lowy float, highy float, lowtime timestamptz, 
  hightime timestamptz, maxminutes int, mincard int, maxcard int, 
  srid int DEFAULT 0, fixstart bool DEFAULT false)
  RETURNS tjsonb AS $$
DECLARE
  tsarr timestamptz[];
  result tjsonb[];
  card int;
  t1 timestamptz;
  interp text;
  lower_inc boolean;
  upper_inc boolean;
BEGIN
  SELECT random_timestamptz_array(lowtime, hightime, maxminutes, mincard,
    maxcard, fixstart) INTO tsarr;
  card = array_length(tsarr, 1);
  IF card = 1 THEN
    lower_inc = true;
    upper_inc = true;
  ELSE
    lower_inc = random() > 0.5;
    upper_inc = random() > 0.5;
  END IF;
  FOR i IN 1..card - 1
  LOOP
    result[i] = tjsonb(random_jsonb(lowId, highId, lowtime, hightime, lowx,
      highx, lowy, highy, srid), tsarr[i]);
  END LOOP;
  -- Sequences with step interpolation and exclusive upper bound must have
  -- the same value in the last two instants
  IF card <> 1 AND NOT upper_inc THEN
    result[card] = tjsonb(getValue(result[card - 1]), tsarr[card]);
  ELSE
    result[card] = tjsonb(random_jsonb(lowId, highId, lowtime, hightime, lowx,
      highx, lowy, highy, srid), tsarr[card]);
  END IF;
  RETURN tjsonbSeq(result, 'Step', lower_inc, upper_inc);
END;
$$ LANGUAGE PLPGSQL STRICT;

/*
SELECT k, random_tjsonb_contseq(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 10, 10, 10)
FROM generate_series (1, 15) AS k;

SELECT k, random_tjsonb_contseq(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 10, 10, 10, 5676)
FROM generate_series (1, 15) AS k;
*/

-------------------------------------------------------------------------------

/**
 * @brief Generate a random temporal JSONB of sequence set subtype
 * @param[in] lowId, highId Inclusive bounds of the range for the vehicleId
 * @param[in] lowx, highx Inclusive bounds of the range for the x coordinates
 * @param[in] lowy, highy Inclusive bounds of the range for the y coordinates
 * @param[in] lowtime, hightime Inclusive bounds of the tstzspan
 * @param[in] maxminutes Maximum number of minutes between consecutive instants
 * @param[in] mincardseq, maxcardseq Inclusive bounds of the number of instants 
 *   in a sequence
 * @param[in] mincard, maxcard Inclusive bounds of the number of sequences
 * @param[in] srid Optional SRID for the point of the jsonb
 */
DROP FUNCTION IF EXISTS random_tjsonb_seqset;
CREATE FUNCTION random_tjsonb_seqset(lowId int, highId int, lowx float,
  highx float, lowy float, highy float, lowtime timestamptz, 
  hightime timestamptz, maxminutes int, mincardseq int, maxcardseq int,
  mincard int, maxcard int, srid int DEFAULT 0)
  RETURNS tjsonb AS $$
DECLARE
  result tjsonb[];
  card int;
  seq tjsonb;
  t1 timestamptz;
  t2 timestamptz;
BEGIN
  PERFORM tsequenceset_valid_duration(lowtime, hightime, maxminutes, mincardseq,
    maxcardseq, mincard, maxcard);
  card = random_int(mincard, maxcard);
  t1 = lowtime;
  t2 = hightime - interval '1 minute' *
    ( (maxminutes * (maxcardseq - mincardseq) * (maxcard - mincard)) +
    ((maxcard - mincard) * maxminutes) );
  FOR i IN 1..card
  LOOP
    -- the last parameter (fixstart) is set to true for all i except 1
    SELECT random_tjsonb_contseq(lowId, highId, lowx, highx, lowy, highy,
      t1, t2, maxminutes, mincardseq, maxcardseq, srid, i > 1) INTO seq;
    result[i] = seq;
    t1 = endTimestamp(seq) + random_minutes(1, maxminutes);
    t2 = t2 + interval '1 minute' * maxminutes * (1 + maxcardseq - mincardseq);
  END LOOP;
  RETURN tjsonbSeqSet(result);
END;
$$ LANGUAGE PLPGSQL STRICT;

/*
SELECT k, random_tjsonb_seqset(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 10, 1, 10, 1, 10) AS seqset
FROM generate_series (1, 15) AS k;

SELECT k, random_tjsonb_seqset(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 10, 1, 10, 1, 10, 5676) AS seqset
FROM generate_series (1, 15) AS k;
*/

-------------------------------------------------------------------------------

