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
 * documentation FOR any purpose, without fee, and without a written
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
 * @brief Function generating test tables for temporal JSONB values
 * @details These functions use the random generator for these types that are
 * in the file `random_tjsonb.sql`. Refer to that file for the meaning of the
 * parameters used in the function calls of this file.
 */

DROP FUNCTION IF EXISTS create_test_tables_tjsonb();
CREATE OR REPLACE FUNCTION create_test_tables_tjsonb(size int DEFAULT 100, 
  perc int DEFAULT 1)
RETURNS text AS $$
BEGIN

------------------------------------------------------------------------------
-- Static pose
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS tbl_jsonb;
CREATE TABLE tbl_jsonb AS
SELECT k, random_jsonb(1, 100, '2001-01-01', '2001-12-31', -100, 100, -100, 100, 3812) AS jb
FROM generate_series(1, size) k;

DROP TABLE IF EXISTS tbl_jsonbset;
CREATE TABLE tbl_jsonbset AS
/* Add perc NULL values */
SELECT k, NULL AS s
FROM generate_series(1, perc) AS k UNION
SELECT k, random_jsonb_set(1, 100, '2001-01-01', '2001-12-31', -100, 100, -100, 100, 1, 10, 3812)
FROM generate_series(perc+1, size) AS k;

------------------------------------------------------------------------------
-- Temporal pose
------------------------------------------------------------------------------

DROP TABLE IF EXISTS tbl_tjsonb_inst;
CREATE TABLE tbl_tjsonb_inst AS
SELECT k, random_tjsonb_inst(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 3812) AS inst
FROM generate_series(1, size) k;

DROP TABLE IF EXISTS tbl_tjsonb_discseq;
CREATE TABLE tbl_tjsonb_discseq AS
SELECT k, random_tjsonb_discseq(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 10, 1, 10, 3812) AS seq
FROM generate_series(1, size) k;

DROP TABLE IF EXISTS tbl_tjsonb_seq;
CREATE TABLE tbl_tjsonb_seq AS
SELECT k, random_tjsonb_contseq(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 10, 1, 10, 3812) AS seq
FROM generate_series(1, size) k;

DROP TABLE IF EXISTS tbl_tjsonb_seqset;
CREATE TABLE tbl_tjsonb_seqset AS
SELECT k, random_tjsonb_seqset(1, 100, -100, 100, -100, 100,
  '2001-01-01', '2001-12-31', 10, 1, 10, 1, 10, 3812) AS ss
FROM generate_series(1, size) AS k;

DROP TABLE IF EXISTS tbl_tjsonb;
CREATE TABLE tbl_tjsonb(k, temp) AS
(SELECT k, inst FROM tbl_tjsonb_inst LIMIT size / 4) UNION ALL
(SELECT k + size / 4, seq FROM tbl_tjsonb_discseq LIMIT size / 4) UNION ALL
(SELECT k + size / 2, seq FROM tbl_tjsonb_seq LIMIT size / 4) UNION ALL
(SELECT k + size / 4 * 3, ss FROM tbl_tjsonb_seqset LIMIT size / 4);

-------------------------------------------------------------------------------
RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

-- SELECT create_test_tables_tjsonb(100);
/*
SELECT * FROM tbl_jsonb LIMIT 3;
SELECT * FROM tbl_jsonbset LIMIT 3;
SELECT * FROM tbl_tjsonb_inst LIMIT 3;
SELECT * FROM tbl_tjsonb_discseq LIMIT 3;
SELECT * FROM tbl_tjsonb_seq LIMIT 3;
SELECT * FROM tbl_tjsonb_seqset LIMIT 3;
SELECT * FROM tbl_tjsonb LIMIT 3;
SELECT * FROM tbl_tjsonb LIMIT 3 OFFSET 25;
SELECT * FROM tbl_tjsonb LIMIT 3 OFFSET 50;
SELECT * FROM tbl_tjsonb LIMIT 3 OFFSET 75;  
*/
