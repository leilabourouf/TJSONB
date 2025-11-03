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

typedef enum
{
  DELETE_KEY_ARRAY,
  EXISTS_KEY_ARRAY,
  SET_PATH_ARRAY,
  DELETE_PATH,
  INSERT_PATH,
  EXTRACT_PATH,
  EXTRACT_PATH_TEXT,
} JsonbFunc;

extern bool jsonb_exists_internal(const Jsonb *jb, const text *key);
extern bool jsonb_exists_array(const Jsonb *jb, const text **keys_elems,
  int keys_len, bool any);
extern bool jsonb_contains_internal(const Jsonb *jb1, const Jsonb *jb2);
extern bool jsonb_contained_internal(const Jsonb *jb1, const Jsonb *jb2);

extern bool jsonb_eq_internal(const Jsonb *jb1, const Jsonb *jb2);
extern bool jsonb_ne_internal(const Jsonb *jb1, const Jsonb *jb2);
extern bool jsonb_gt_internal(const Jsonb *jb1, const Jsonb *jb2);
extern bool jsonb_lt_internal(const Jsonb *jb1, const Jsonb *jb2);
extern bool jsonb_ge_internal(const Jsonb *jb1, const Jsonb *jb2);
extern bool jsonb_le_internal(const Jsonb *jb1, const Jsonb *jb2);
extern int jsonb_cmp_internal(const Jsonb *jb1, const Jsonb *jb2);

extern uint32 jsonb_hash_internal(Jsonb *key);
extern uint64 jsonb_hash_extended_internal(Jsonb *key, uint64 seed);

extern Jsonb *jsonb_object_field_internal(const Jsonb *jb, const text *key);
extern text *jsonb_object_field_text_internal(const Jsonb *jb, const text *key);
extern Jsonb *jsonb_extract_path_internal(const Jsonb *jb, Datum *pathtext,
  int path_len);
extern Jsonb *jsonb_extract_path_text_internal(const Jsonb *jb,
  Datum *pathtext, int path_len);

extern Jsonb *concat_jsonb_jsonb(const Jsonb *jb1, const Jsonb *jb2);
extern Jsonb *jsonb_delete_internal(const Jsonb *jb, const text *key);
extern Jsonb *jsonb_delete_key_array_internal(const Jsonb *jb,
  const text **keys_elems, bool *keys_nulls, int keys_len);
extern Jsonb *jsonb_delete_path_internal(const Jsonb *jb, Datum *path_elems,
  bool *path_nulls, int path_len);
extern Jsonb *jsonb_extract_path_internal(const Jsonb *jb, Datum *path_elems,
  int path_len);

extern Jsonb *jsonb_set_internal(const Jsonb *jb, Datum *path_elems,
  bool *path_nulls, int path_len, Jsonb *newjsonb, bool create);
extern Jsonb *jsonb_insert_internal(const Jsonb *jb, Datum *path_elems,
  bool *path_nulls, int path_len, Jsonb *newjsonb, bool create);

/*----------------------------------------------------------------------------
 * Datum‐level JSONB operations
 *---------------------------------------------------------------------------*/

Datum datum_jsonb_object_field(Datum l, Datum r);
Datum datum_jsonb_object_field_text(Datum l, Datum r);

extern Datum datum_jsonb_exists(Datum l, Datum r);
extern Datum datum_jsonb_contains(Datum l, Datum r);
extern Datum datum_jsonb_contained(Datum l, Datum r);

extern Datum datum_jsonb_to_text(Datum jb);
extern Datum datum_text_to_jsonb(Datum txt);
extern Datum datum_jsonb_concat(Datum left, Datum right);
extern Datum datum_jsonb_delete(Datum left, Datum right);
extern Datum datum_jsonb_delete_idx(Datum left, Datum right);

/*----------------------------------------------------------------------------
 * Set wrappers for JSONB operations
 *---------------------------------------------------------------------------*/

extern Set *jsonbfunc_jsonbset(const Set *s, datum_func1 func, meosType intype,
  meosType restype);
extern Set *jsonbfunc_jsonbset_jsonb(const Set *s, const Jsonb *jb,
  datum_func2 func, bool invert);
extern Set *jsonbfunc_jsonbset_text(const Set *s, const text *txt,
  datum_func2 func);

/*----------------------------------------------------------------------------
 * Temporal wrappers for JSONB operations
 *---------------------------------------------------------------------------*/

extern Temporal *jsonbfunc_tjsonb(const Temporal *temp, Datum *params, 
  varfunc func, meosType intype, meosType paramtype, meosType restype);
extern Temporal *jsonbfunc_tjsonb_jsonb(const Temporal *temp, Datum value,
  meosType valuetype, datum_func2 func, meosType restype, bool invert);
extern Temporal *jsonbfunc_tjsonb_tjsonb(const Temporal *temp1,
  const Temporal *temp2, datum_func2 func, meosType restype);
extern Temporal *jsonbfunc_tjsonb_text(const Temporal *temp, Datum value,
  datum_func2 func, meosType restype, bool invert);

/*---------------------------------------------------------------------------*/

extern Temporal *tjsonb_func_textarr(const Temporal *temp, const text **elems,
  bool *nulls, int count, Datum *params, JsonbFunc func);
  
/*****************************************************************************/

#endif 
