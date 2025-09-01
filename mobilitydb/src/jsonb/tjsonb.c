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
 * @brief Temporal distance for temporal circular buffers
 */

/* PostgreSQL */
#include <postgres.h>
#include "utils/jsonb.h"
/* MEOS */
#include <meos.h>
#include "temporal/span.h"
#include "jsonb/tjsonb_funcs.h"
/* MobilityDB */
#include "pg_temporal/temporal.h"

extern Datum jsonb_set(PG_FUNCTION_ARGS);

/*****************************************************************************
 * JSONB concatenation
 *****************************************************************************/

PGDLLEXPORT Datum Concat_jsonb_tjsonb(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Concat_jsonb_tjsonb);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Concat a JSONB constant with a temporal JSONB
 * @sqlfn jsonb_concat()
 * @sqlop @p ||
 */
Datum
Concat_jsonb_tjsonb(PG_FUNCTION_ARGS)
{
  Datum jb = PG_GETARG_DATUM(0);
  Temporal *temp  = PG_GETARG_TEMPORAL_P(1);
  Temporal *result = jsonbfunc_tjsonb_jsonb(temp, jb, &datum_jsonb_concat,
    true);
  PG_FREE_IF_COPY(temp, 1);
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Concat_tjsonb_jsonb(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Concat_tjsonb_jsonb);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Concat a temporal JSONB with a JSONB constant
 * @sqlfn jsonb_concat()
 * @sqlop @p ||
 */
Datum
Concat_tjsonb_jsonb(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  Datum jb = PG_GETARG_DATUM(1);
  Temporal *result = jsonbfunc_tjsonb_jsonb(temp, jb, &datum_jsonb_concat,
    false);
  PG_FREE_IF_COPY(temp, 0);
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Concat_tjsonb_tjsonb(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Concat_tjsonb_tjsonb);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Concat two temporal JSONB values
 * @sqlfn jsonb_concat()
 * @sqlop @p ||
 */
Datum
Concat_tjsonb_tjsonb(PG_FUNCTION_ARGS)
{
  Temporal *temp1 = PG_GETARG_TEMPORAL_P(0);
  Temporal *temp2 = PG_GETARG_TEMPORAL_P(1);
  Temporal *result = jsonbfunc_tjsonb_tjsonb(temp1, temp2,
    &datum_jsonb_concat);
  PG_FREE_IF_COPY(temp1, 0);
  PG_FREE_IF_COPY(temp2, 1);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Delete_tjsonb_key(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Delete_tjsonb_key);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Delete a key from a temporal JSONB value
 * @sqlfn jsonb_delete()
 * @sqlop @p -
 */
Datum
Delete_tjsonb_key(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  text *key = PG_GETARG_TEXT_P(1);
  Temporal *result = jsonbfunc_tjsonb_text(temp, key,
    &datum_jsonb_delete, INVERT_NO);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(key, 1);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

/*****************************************************************************
 * JSONB set 
 *****************************************************************************/

/* ---------- Pack jsonb_set extra args to pass through the 2-arg mapper ----- */
typedef struct JsonbSetPackedArgs
{
  ArrayType *path;      /* text[] */
  Jsonb     *val;       /* jsonb  */
  bool       create_missing;
} JsonbSetPackedArgs;

/* 2-arg adapter for the mapper: left = per-instant jsonb, right = packed args */
static Datum
datum_jsonb_set_packed(Datum left, Datum right)
{
  JsonbSetPackedArgs *args = (JsonbSetPackedArgs *) DatumGetPointer(right);
  return DirectFunctionCall4(jsonb_set, left,
    PointerGetDatum(args->path),  /* text[] */
    PointerGetDatum(args->val),   /* jsonb  */
    BoolGetDatum(args->create_missing));
}

/* ------------------------------- SQL wrapper ------------------------------- */
PGDLLEXPORT Datum Tjsonb_set(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_set);
Datum
Tjsonb_set(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  ArrayType *path = PG_GETARG_ARRAYTYPE_P(1); /* text[] */
  Jsonb *val  = PG_GETARG_JSONB_P(2);
  bool create_missing = PG_GETARG_BOOL(3);

  /* Pack once; mapper will reuse for each instant */
  JsonbSetPackedArgs *packed =
    (JsonbSetPackedArgs *) palloc(sizeof(JsonbSetPackedArgs));
  packed->path = path;
  packed->val = val;
  packed->create_missing = create_missing;
  Temporal *res = jsonbfunc_tjsonb_jsonb(temp, PointerGetDatum(packed),
    &datum_jsonb_set_packed, false);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(path, 1);
  PG_RETURN_TEMPORAL_P(res);
}

/*****************************************************************************/
