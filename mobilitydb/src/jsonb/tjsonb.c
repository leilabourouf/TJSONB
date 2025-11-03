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

/* PostgreSQL */
#include <postgres.h>
#include "utils/jsonb.h"
#include "utils/jsonpath.h"
#include "executor/spi.h"

#include "utils/jsonpath.h"
#include "executor/spi.h"

/* MEOS */
#include <meos.h>
#include <meos_jsonb.h>
#include "temporal/span.h"
#include "jsonb/tjsonb_funcs.h"
/* MobilityDB */
#include "pg_temporal/temporal.h"

extern Datum jsonb_in(PG_FUNCTION_ARGS);

/*****************************************************************************
 * JSONB casting
 *****************************************************************************/

PGDLLEXPORT Datum Jsonb_as_text(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Jsonb_as_text);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Transform a JSONB value into a text value
 * @sqlfn text()
 * @sqlop @p ::
 */
Datum
Jsonb_as_text(PG_FUNCTION_ARGS)
{
  Jsonb *jb = PG_GETARG_JSONB_P(0);
  char *str = pg_jsonb_out(jb);
  text *result = cstring2text(str);
  pfree(str);
  PG_RETURN_TEXT_P(result);
}

PGDLLEXPORT Datum Tjsonb_as_ttext(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_as_ttext);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Transform a temporal JSONB value into a temporal text value
 * @sqlfn ttext()
 * @sqlop @p ::
 */
Datum
Tjsonb_as_ttext(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  Temporal *result = jsonbfunc_tjsonb(temp, (Datum) 0, 
    (varfunc) &datum_jsonb_to_text, T_TJSONB, 0, T_TTEXT);
  PG_FREE_IF_COPY(temp, 0);
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Ttext_as_tjsonb(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Ttext_as_tjsonb);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Transform a temporal JSONB value into a temporal text value
 * @sqlfn ttext()
 * @sqlop @p ::
 */
Datum
Ttext_as_tjsonb(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  Temporal *result = jsonbfunc_tjsonb(temp, (Datum) 0,
    (varfunc) &datum_text_to_jsonb, T_TTEXT, 0, T_TJSONB);
  PG_FREE_IF_COPY(temp, 0);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

/*****************************************************************************
 * JSONB functions
 *****************************************************************************/

PGDLLEXPORT Datum Tjsonb_object_field(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_object_field);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Extract a field from each JSONB value in a temporal JSONB
 * @sqlfn tjsonb_object_field()
 * @sqlop @p ->
 */
Datum
Tjsonb_object_field(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  text *key = PG_GETARG_TEXT_P(1);
  Temporal *result = jsonbfunc_tjsonb_text(temp, PointerGetDatum(key),
    &datum_jsonb_object_field, T_TJSONB, false);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(key, 1);
  if (!result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Tjsonb_object_field_text(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_object_field_text);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Extract a field from each JSONB value in a temporal JSONB
 * @sqlfn tjsonb_object_field_text()
 * @sqlop @p ->>
 */
Datum
Tjsonb_object_field_text(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  text *key = PG_GETARG_TEXT_P(1);
  Temporal *result = jsonbfunc_tjsonb_text(temp, PointerGetDatum(key),
    &datum_jsonb_object_field_text, T_TTEXT, true);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(key, 1);
  if (!result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Concat_jsonb_tjsonb(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Concat_jsonb_tjsonb);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Concat a JSONB constant with a temporal JSONB
 * @sqlfn tjsonb_concat()
 * @sqlop @p ||
 */
Datum
Concat_jsonb_tjsonb(PG_FUNCTION_ARGS)
{
  Datum jb = PG_GETARG_DATUM(0);
  Temporal *temp  = PG_GETARG_TEMPORAL_P(1);
  Temporal *result = jsonbfunc_tjsonb_jsonb(temp, jb, T_TJSONB,
    &datum_jsonb_concat, T_TJSONB, true);
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
  Temporal *result = jsonbfunc_tjsonb_jsonb(temp, jb, T_TJSONB,
    &datum_jsonb_concat, T_TJSONB, false);
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
    &datum_jsonb_concat, T_TJSONB);
  PG_FREE_IF_COPY(temp1, 0);
  PG_FREE_IF_COPY(temp2, 1);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

/*****************************************************************************/

PGDLLEXPORT Datum Tjsonb_delete_key(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_delete_key);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Delete a key from a temporal JSONB value
 * @sqlfn jsonb_delete()
 * @sqlop @p -
 */
Datum
Tjsonb_delete_key(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  text *key = PG_GETARG_TEXT_P(1);
  Datum params[1];
  params[0] = PointerGetDatum(key);
  Temporal *result = jsonbfunc_tjsonb(temp, params,
    (varfunc) &datum_jsonb_delete, T_TJSONB, T_TEXT, T_TJSONB);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(key, 1);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Tjsonb_delete_key_array(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_delete_key_array);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Delete an array of keys from a temporal JSONB value
 * @sqlfn jsonb_delete_array()
 * @sqlop @p -
 */
Datum
Tjsonb_delete_key_array(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  ArrayType *keys = PG_GETARG_ARRAYTYPE_P(1);
  if (ARR_NDIM(keys) > 1)
    ereport(ERROR,
      (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
       errmsg("wrong number of array subscripts")));

  /* Extract the keys from the array */
  int keys_len;
  Datum *keys_elems;
  bool *keys_nulls;
  deconstruct_array_builtin(keys, TEXTOID, &keys_elems, &keys_nulls, &keys_len);
  if (keys_len == 0)
    PG_RETURN_TEMPORAL_P(temp);

  /* Compute the result */
  Datum params = (Datum) 0;
  Temporal *result = tjsonb_func_textarr(temp, (const text**) keys_elems,
    keys_nulls, keys_len, &params, DELETE_KEY_ARRAY);

  pfree(keys_elems);
  pfree(keys_nulls);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(keys, 1);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Tjsonb_delete_idx(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_delete_idx);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Delete a key from a temporal JSONB value
 * @sqlfn jsonb_delete()
 * @sqlop @p -
 */
Datum
Tjsonb_delete_idx(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  int idx = PG_GETARG_INT32(1);
  Datum params[1];
  params[0] = Int32GetDatum(idx);
  Temporal *result = jsonbfunc_tjsonb(temp, params, 
    (varfunc) &datum_jsonb_delete_idx, T_TJSONB, T_INT4, T_TJSONB);
  PG_FREE_IF_COPY(temp, 0);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

/**
 * @brief Extract a path from a temporal JSONB value
 */
Datum
Tjsonb_extract_path_ext(FunctionCallInfo fcinfo, bool as_text)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  ArrayType *path = PG_GETARG_ARRAYTYPE_P(1);
  if (ARR_NDIM(path) > 1)
    ereport(ERROR,
      (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
       errmsg("wrong number of array subscripts")));

  /* Extract the path from the array */
  int path_len;
  Datum *path_elems;
  bool *path_nulls;
  deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);
  if (path_len == 0)
    PG_RETURN_TEMPORAL_P(temp);
  /*
   * If the array contains any null elements, return NULL, on the grounds
   * that you'd have gotten NULL if any RHS value were NULL in a nested
   * series of applications of the -> operator.  (Note: because we also
   * return NULL for error cases such as no-such-field, this is true
   * regardless of the contents of the rest of the array.)
   */
  if (array_contains_nulls(path))
    PG_RETURN_NULL();
  
  /* Compute the result */
  Datum params = (Datum) 0;
  Temporal *result = tjsonb_func_textarr(temp, (const text**) path_elems,
    NULL, path_len, &params, as_text ? EXTRACT_PATH_TEXT : EXTRACT_PATH);

  pfree(path_elems);
  pfree(path_nulls);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(path, 1);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Tjsonb_extract_path(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_extract_path);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Extract a path from a temporal JSONB value
 * @sqlfn Tjsonb_delete_path()
 */
Datum
Tjsonb_extract_path(PG_FUNCTION_ARGS)
{
  return Tjsonb_extract_path_ext(fcinfo, false);
}

PGDLLEXPORT Datum Tjsonb_extract_path_text(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_extract_path_text);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Extract a path from a temporal JSONB value
 * @sqlfn Tjsonb_delete_path()
 */
Datum
Tjsonb_extract_path_text(PG_FUNCTION_ARGS)
{
  return Tjsonb_extract_path_ext(fcinfo, true);
}

/*****************************************************************************/

PGDLLEXPORT Datum Tjsonb_set(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_set);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Replace a JSONB value specified by a path with a new value
 * @sqlfn tjsonb_set()
 */
Datum
Tjsonb_set(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  ArrayType *path = PG_GETARG_ARRAYTYPE_P(1);
  Jsonb *newjsonb = PG_GETARG_JSONB_P(2);
  bool create = PG_GETARG_BOOL(3);
  if (ARR_NDIM(path) > 1)
    ereport(ERROR,
        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
         errmsg("wrong number of array subscripts")));

  Datum *path_elems;
  bool *path_nulls;
  int path_len;
  deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);
  if (path_len == 0)
    PG_RETURN_TEMPORAL_P(temp);

  /* Compute the result */
  Datum params[2];
  params[0] = PointerGetDatum(newjsonb);
  params[1] = BoolGetDatum(create);
  Temporal *result = tjsonb_func_textarr(temp, (const text **) path_elems,
    path_nulls, path_len, params, DELETE_KEY_ARRAY);

  pfree(path_elems);
  pfree(path_nulls);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(path, 1);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Tjsonb_delete_path(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_delete_path);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Delete a path from a temporal JSONB value
 * @sqlfn Tjsonb_delete_path()
 */
Datum
Tjsonb_delete_path(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  ArrayType *path = PG_GETARG_ARRAYTYPE_P(1);
  if (ARR_NDIM(path) > 1)
    ereport(ERROR,
      (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
       errmsg("wrong number of array subscripts")));

  /* Extract the path from the array */
  int path_len;
  Datum *path_elems;
  bool *path_nulls;
  deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);
  if (path_len == 0)
    PG_RETURN_TEMPORAL_P(temp);

  /* Compute the result */
  Datum params = (Datum) 0;
  Temporal *result = tjsonb_func_textarr(temp, (const text**) path_elems,
    path_nulls, path_len, &params, DELETE_PATH);

  pfree(path_elems);
  pfree(path_nulls);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(path, 1);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

PGDLLEXPORT Datum Tjsonb_insert(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_insert);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Insert a path into a temporal JSONB value
 * @sqlfn tjsonb_insert()
 */
Datum
Tjsonb_insert(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  ArrayType *path = PG_GETARG_ARRAYTYPE_P(1);
  Jsonb *newjsonb = PG_GETARG_JSONB_P(2);
  bool after = PG_GETARG_BOOL(3);
  if (ARR_NDIM(path) > 1)
    ereport(ERROR,
      (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
       errmsg("wrong number of array subscripts")));

  /* Extract the path from the array */
  int path_len;
  Datum *path_elems;
  bool *path_nulls;
  deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);
  if (path_len == 0)
    PG_RETURN_TEMPORAL_P(temp);

  /* Compute the result */
  Datum params[2];
  params[0] = PointerGetDatum(newjsonb);
  params[1] = BoolGetDatum(after);
  Temporal *result = tjsonb_func_textarr(temp, (const text**) path_elems,
    path_nulls, path_len, params, INSERT_PATH);

  pfree(path_elems);
  pfree(path_nulls);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(path, 1);
  if (! result)
    PG_RETURN_NULL();
  PG_RETURN_TEMPORAL_P(result);
}

/*****************************************************************************/

PGDLLEXPORT Datum Tjsonb_to_tfloat(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_to_tfloat);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief SQL wrapper to convert a temporal JSONB into a temporal float
 * @sqlfn tjsonb_to_tfloat()
 * Extracts the given key from each JSONB value and delegates to
 * @ref tjsonb_to_tfloat_internal() for the actual conversion.
 */
Datum
Tjsonb_to_tfloat(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  text *key_text = PG_GETARG_TEXT_PP(1);
  char *key = text2cstring(key_text);
  Temporal *result = tjsonb_to_tfloat_internal(temp, key);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(key_text, 1);
  PG_RETURN_POINTER(result);
}


PGDLLEXPORT Datum Tjsonb_twavg(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_twavg);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Compute the time-weighted average of a temporal JSONB for a given key
 * @sqlfn twAvg()
 * @sqlop @p avg
 */
Datum
Tjsonb_twavg(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  text *key_text = PG_GETARG_TEXT_PP(1);
  char *key = text2cstring(key_text);
  double result = tjsonb_twavg_internal(temp, key);
  PG_FREE_IF_COPY(temp, 0);
  PG_FREE_IF_COPY(key_text, 1);
  PG_RETURN_FLOAT8(result);
}

/*****************************************************************************
 * JSONB path query
 *****************************************************************************/

PGDLLEXPORT Datum Tjsonb_path_extract(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_path_extract);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Extract values from a temporal JSONB using a JSONPath expression
 * @sqlfn tjsonb_path_extract()
 * This function applies a JSONPath expression to each instant of a temporal
 * JSONB and returns a new temporal JSONB with the extracted values.
 * Implementation details:
 * - PostgreSQL 14 version: relies on SPI and calls
 *   @p jsonb_path_query_first($1, $2).
 * - If the path does not exist, a JSON null value is returned.
 * @param[in] temp   Temporal JSONB input
 * @param[in] jspath JSONPath expression to apply
 * @return A new temporal JSONB sequence containing the extracted values
 * @note In PostgreSQL 15 and higher, @p jsonb_path_query_first_typed can be
 *       used directly instead of going through SPI.
 */
Datum
Tjsonb_path_extract(PG_FUNCTION_ARGS)
{
    Temporal *temp = PG_GETARG_TEMPORAL_P(0);
    JsonPath *jspath = PG_GETARG_JSONPATH_P(1);
    int count;
    const TInstant **insts = temporal_instants_p(temp, &count);
    TInstant **new_insts = palloc(sizeof(TInstant *) * count);
    /* Start SPI context (needed to call SQL function safely) */
    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("SPI_connect failed")));
    Oid argtypes[2] = {JSONBOID, JSONPATHOID};

    for (int i = 0; i < count; i++)
    {
        Datum val = tinstant_value(insts[i]);
        Datum values[2] = {val, PointerGetDatum(jspath)};
        char nulls[2] = {' ', ' '};
        Datum newval;
        int ret = SPI_execute_with_args(
            "SELECT jsonb_path_query_first($1, $2)",
            2, argtypes, values, nulls,
            true, 1);
        if (ret == SPI_OK_SELECT && SPI_processed > 0)
        {
            HeapTuple tup = SPI_tuptable->vals[0];
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            bool isnull;
            newval = SPI_getbinval(tup, tupdesc, 1, &isnull);
            if (isnull)
                newval = DirectFunctionCall1(jsonb_in, CStringGetDatum("null"));
        }
        else
        {
            newval = DirectFunctionCall1(jsonb_in, CStringGetDatum("null"));
        }
        new_insts[i] = tinstant_make(newval, T_TJSONB, insts[i]->t);
    }
    SPI_finish();
    /* Build result sequence */
    TSequence *result = tsequence_make(
                            (const TInstant **) new_insts,
                            count,
                            true, true, STEP, NORMALIZE);

    PG_FREE_IF_COPY(temp, 0);
    PG_RETURN_TSEQUENCE_P(result);
}

PGDLLEXPORT Datum Tjsonb_lag(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_lag);
/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Return the value of a temporal JSONB lagged by a given offset
 * @sqlfn tjsonb_lag()
 * @sqlop @p lag
 * This function shifts the values of a temporal JSONB backwards in time by
 * a specified number of instants (default = 1). Positions without a value
 * (i.e., before the start) are filled with JSON null.
 * @param[in] temp Temporal JSONB input
 * @param[in] offset Number of instants to lag (optional, default = 1)
 * @return A new temporal JSONB sequence with lagged values
 */
Datum
Tjsonb_lag(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  int offset = PG_NARGS() > 1 ? PG_GETARG_INT32(1) : 1;
  int count;
  const TInstant **insts = temporal_instants_p(temp, &count);
  TInstant **new_insts = palloc(sizeof(TInstant *) * count);
  for (int i = 0; i < count; i++)
  {
    Datum val;
    if (i < offset)
    {
      /* JSON null for missing lagged values */
      val = DirectFunctionCall1(jsonb_in, CStringGetDatum("null"));
    }
    else
    {
      val = tinstant_value(insts[i - offset]);
    }
    new_insts[i] = tinstant_make(val, T_TJSONB, insts[i]->t);
  }
  TSequence *result = tsequence_make(
                        (const TInstant **) new_insts,
                        count,
                        true, true, STEP, NORMALIZE);
  PG_FREE_IF_COPY(temp, 0);
  PG_RETURN_TSEQUENCE_P(result);
}

PGDLLEXPORT Datum Tjsonb_lead(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(Tjsonb_lead);

/**
 * @ingroup mobilitydb_temporal_jsonb
 * @brief Return the value of a temporal JSONB shifted forward by a given offset
 * @sqlfn tjsonb_lead()
 * @sqlop @p lead
 * This function shifts the values of a temporal JSONB forward in time by
 * a specified number of instants (default = 1). Positions without a value
 * (i.e., beyond the end) are filled with JSON null.
 * @param[in] temp   Temporal JSONB input
 * @param[in] offset Number of instants to lead (optional, default = 1)
 * @return A new temporal JSONB sequence with lead values
 */
Datum
Tjsonb_lead(PG_FUNCTION_ARGS)
{
  Temporal *temp = PG_GETARG_TEMPORAL_P(0);
  int offset = PG_NARGS() > 1 ? PG_GETARG_INT32(1) : 1;
  int count;
  const TInstant **insts = temporal_instants_p(temp, &count);
  TInstant **new_insts = palloc(sizeof(TInstant *) * count);
  for (int i = 0; i < count; i++)
  {
    Datum val;
    if (i + offset >= count)
    {
      /* JSON null for missing lead values */
      val = DirectFunctionCall1(jsonb_in, CStringGetDatum("null"));
    }
    else
    {
      val = tinstant_value(insts[i + offset]);
    }
    new_insts[i] = tinstant_make(val, T_TJSONB, insts[i]->t);
  }
  TSequence *result = tsequence_make(
                        (const TInstant **) new_insts,
                        count,
                        true, true, STEP, NORMALIZE);
  PG_FREE_IF_COPY(temp, 0);
  PG_RETURN_TSEQUENCE_P(result);
}