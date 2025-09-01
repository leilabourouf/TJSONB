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
 * @brief Basic functions for temporal JSONB
 */

#ifndef __TJSONB_JSONBFUNCS_H__
#define __TJSONB_JSONBFUNCS_H__

/* PostgreSQL */
#include <postgres.h>
#include <utils/jsonb.h>
/* MEOS */
#include <meos.h>
#include "temporal/temporal.h"

/* Operations available for setPath */
#define JB_PATH_CREATE					0x0001
#define JB_PATH_DELETE					0x0002
#define JB_PATH_REPLACE					0x0004
#define JB_PATH_INSERT_BEFORE			0x0008
#define JB_PATH_INSERT_AFTER			0x0010
#define JB_PATH_CREATE_OR_INSERT \
	(JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER | JB_PATH_CREATE)
#define JB_PATH_FILL_GAPS				0x0020
#define JB_PATH_CONSISTENT_POSITION		0x0040

/*----------------------------------------------------------------------------
 * Internal JSONB operations
 *---------------------------------------------------------------------------*/

extern Jsonb *concat_jsonb_jsonb(const Jsonb *jb1, const Jsonb *jb2);
extern Jsonb *jsonb_delete_internal(const Jsonb *in, const text *key);
extern Jsonb *jsonb_set_internal(const Jsonb *in, Datum *path_elems,
  int path_len, Jsonb *newjsonb, bool create);

/*----------------------------------------------------------------------------
 * Datum‐level JSONB operations
 *---------------------------------------------------------------------------*/

/** Concatenate two JSONB values (objects or arrays) */
extern Datum datum_jsonb_concat(Datum left, Datum right);
/** Delete a JSONB value from another */
extern Datum datum_jsonb_delete(Datum left, Datum right);

/*----------------------------------------------------------------------------
 * Temporal wrappers for JSONB operations
 *---------------------------------------------------------------------------*/

// Apply a unary JSONB→JSONB function to each instant of a T_TJSONB.
extern Temporal *jsonbfunc_tjsonb(const Temporal *temp, datum_func1 func);

// Apply a binary JSONB→JSONB function between each instant and a constant.
extern Temporal *jsonbfunc_tjsonb_text(const Temporal *temp, const text *txt, datum_func2 func, bool invert);

// Apply a binary JSONB→JSONB function between each instant and a constant.
extern Temporal *jsonbfunc_tjsonb_jsonb(const Temporal *temp, Datum value, datum_func2 func, bool invert);

 //Apply a binary JSONB→JSONB function instant‐by‐instant between two T_TJSONB.
extern Temporal *jsonbfunc_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2, datum_func2 func);

#endif 
