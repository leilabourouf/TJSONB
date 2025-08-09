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

/* C */
#include <assert.h>
/* PostgreSQL */
#include <postgres.h>
/* MEOS */
#include <meos.h>
#include <meos_internal.h>
#include <meos_jsonb.h>
#include "temporal/postgres_types.h"
#include "temporal/temporal.h"
#include "temporal/lifting.h"
#include "temporal/type_parser.h"
#include "temporal/type_util.h"
#include "jsonb/tjsonb_funcs.h"

/* TODO REMOVE TO AVOID CALLING POSTGRESQL FUNCTIONS DIRECTLY */
#if ! MEOS
extern Datum jsonb_in(PG_FUNCTION_ARGS);
extern Datum numeric_out(PG_FUNCTION_ARGS);
#endif /* ! MEOS */

/*****************************************************************************
 * Input/output functions
 *****************************************************************************/

#if MEOS
/**
 * @ingroup meos_jsonb_inout
 * @brief Return a temporal JSONB from its Well-Known Text (WKT) representation
 * @param[in] str String
 */
Temporal *
tjsonb_in(const char *str)
{
  /* Ensure the validity of the arguments */
  VALIDATE_NOT_NULL(str, NULL);
  return temporal_in(str, T_TJSONB);
}

/**
 * @ingroup meos_internal_jsonb_inout
 * @brief Return a temporal JSONB instant from its Well-Known Text (WKT)
 * representation
 * @param[in] str String
 */
TInstant *
tjsonbinst_in(const char *str)
{
  /* Ensure the validity of the arguments */
  VALIDATE_NOT_NULL(str, NULL);
  /* Parse the WKT into a TInstant, telling the parser this is a JSONB instant */
  return tinstant_in(str, T_TJSONB);
}

/**
 * @ingroup meos_internal_jsonb_inout
 * @brief Return a temporal JSONB sequence from its Well-Known Text (WKT)
 * representation
 * @param[in] str String
 * @param[in] interp Interpolation
 */
inline TSequence *
tjsonbseq_in(const char *str, interpType interp UNUSED)
{
  /* Ensure the validity of the arguments */
  VALIDATE_NOT_NULL(str, NULL);
  /* Call the superclass function */
  Temporal *temp = temporal_parse(&str, T_TJSONB);
  assert(temp->subtype == TSEQUENCE);
  return (TSequence *) temp;
}

/**
 * @ingroup meos_internal_jsonb_inout
 * @brief Return a temporal JSONB sequence set from its Well-Known Text (WKT)
 * representation
 * @param[in] str String
 */
TSequenceSet *
tjsonbseqset_in(const char *str)
{
  /* Ensure the validity of the arguments */
  VALIDATE_NOT_NULL(str, NULL);
  /* Call the superclass function */
  Temporal *temp = temporal_parse(&str, T_TJSONB);
  assert(temp->subtype == TSEQUENCESET);
  return (TSequenceSet *) temp;
}
#endif /* MEOS */

/*****************************************************************************/

#if MEOS
/**
 * @ingroup meos_jsonb_inout
 * @brief Return the Well-Known Text (WKT) representation of a temporal JSONB
 * @param[in] temp Temporal JSONB
 */
char *
tjsonb_out(const Temporal *temp)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL);
  return temporal_out(temp, 0);
}
#endif /* MEOS */

/*****************************************************************************/

/**
 * @ingroup meos_internal_jsonb_inout
 * @brief Return a temporal JSONB instant from its MF-JSON representation
 * @param[in] mfjson MFJSON object
 */
inline TInstant *
tjsonbinst_from_mfjson(json_object *mfjson)
{
  /* false = not linear, 0 = unused */
  return tinstant_from_mfjson(mfjson, false, 0, T_TJSONB);
}

/**
 * @ingroup meos_internal_jsonb_inout
 * @brief Return a temporal JSONB sequence from its MF-JSON representation
 * @param[in] mfjson MFJSON object
 */
inline TSequence *
tjsonbseq_from_mfjson(json_object *mfjson)
{
  /* false = not linear, 0 = unused */
  return tsequence_from_mfjson(mfjson, false, 0, T_TJSONB, STEP);
}

/**
 * @ingroup meos_internal_jsonb_inout
 * @brief Return a temporal JSONB sequence set from its MF-JSON representation
 * @param[in] mfjson MFJSON object
 */
inline TSequenceSet *
tjsonbseqset_from_mfjson(json_object *mfjson)
{
  /* false = not linear, 0 = unused */
  return tsequenceset_from_mfjson(mfjson, false, 0, T_TJSONB, STEP);
}

/**
 * @ingroup meos_jsonb_inout
 * @brief Return a temporal JSONB from its MF-JSON representation
 * @param[in] mfjson MFJSON string
 * @return On error return @p NULL
 * @see #temporal_from_mfjson()
 */

Temporal *
tjsonb_from_mfjson(const char *mfjson)
{
  return temporal_from_mfjson(mfjson, T_TJSONB);
}

/*****************************************************************************
 * Constructor functions
 *****************************************************************************/

/**
 * @ingroup meos_jsonb_constructor
 * @brief Return a temporal JSONB instant from a JSONB and a timestamptz
 * @param[in] jsonb Value
 * @param[in] t Timestamp
 * @csqlfn #Tinstant_constructor()
 */
TInstant *
tjsonbinst_make(const Jsonb *jsonb, TimestampTz t)
{
  /* Ensure the validity of the arguments */
  VALIDATE_NOT_NULL(jsonb, NULL);
  return tinstant_make(PointerGetDatum(jsonb), T_TJSONB, t);
}

/**
 * @ingroup meos_jsonb_constructor
 * @brief Return a temporal JSONB discrete sequence from a JSONB value and a
 * timestamptz set
 * @param[in] jsonb Value
 * @param[in] s Set
 * @csqlfn #Tsequence_from_base_tstzset()
 */
TSequence *
tjsonbseq_from_base_tstzset(const Jsonb *jsonb, const Set *s)
{
  /* Ensure the validity of the arguments */
  VALIDATE_NOT_NULL(jsonb, NULL); VALIDATE_TSTZSET(s, NULL);
  return tsequence_from_base_tstzset(PointerGetDatum(jsonb), T_TJSONB, s);
}

/**
 * @ingroup meos_jsonb_constructor
 * @brief Return a temporal JSONB sequence from a JSONB value and a timestamptz
 * span
 * @param[in] jsonb Value
 * @param[in] s Span
 * @csqlfn #Tsequence_from_base_tstzspan()
 */
TSequence *
tjsonbseq_from_base_tstzspan(const Jsonb *jsonb, const Span *s)
{
  /* Ensure the validity of the arguments */
  VALIDATE_NOT_NULL(jsonb, NULL); VALIDATE_TSTZSPAN(s, NULL);
  return tsequence_from_base_tstzspan(PointerGetDatum(jsonb), T_TJSONB, s, STEP);
}

/**
 * @ingroup meos_jsonb_constructor
 * @brief Return a temporal JSONB sequence set from a JSONB value and a timestamptz
 * span set
 * @param[in] jb JSONB value
 * @param[in] ss Span set of timestamptz spans
 * @csqlfn #Tsequenceset_from_base_tstzspanset()
 */
TSequenceSet *
tjsonbseqset_from_base_tstzspanset(const Jsonb *jb, const SpanSet *ss)
{
  /* Ensure the validity of the arguments */
  VALIDATE_NOT_NULL(jb, NULL);
  VALIDATE_TSTZSPANSET(ss, NULL);
  /* Delegate to the generic tsequenceset constructor, with STEP interpolation */
  return tsequenceset_from_base_tstzspanset(PointerGetDatum(jb),T_TJSONB, ss, 
    STEP);
}

/**
 * @ingroup meos_jsonb_constructor
 * @brief Return a temporal JSONB from a JSONB and the time frame of
 * another temporal value
 * @param[in] jsonb Value
 * @param[in] temp Temporal value
 */
Temporal *
tjsonb_from_base_temp(const Jsonb *jsonb, const Temporal *temp)
{
  /* Ensure the validity of the arguments */
  VALIDATE_NOT_NULL(jsonb, NULL); VALIDATE_NOT_NULL(temp, NULL);
  return temporal_from_base_temp(PointerGetDatum(jsonb), T_TJSONB, temp);
}

/*****************************************************************************
 * Conversion functions
 *****************************************************************************/


/*****************************************************************************
 * Accessor functions
 *****************************************************************************/

/**
 * @ingroup meos_jsonb_accessor
 * @brief Return the start value of a temporal JSONB
 * @param[in] temp Temporal value
 * @return On error return @p NULL
 * @csqlfn #Temporal_start_value()
 */
Jsonb *
tjsonb_start_value(const Temporal *temp)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL);
  return DatumGetJsonbP(temporal_start_value(temp));
}

/**
 * @ingroup meos_jsonb_accessor
 * @brief Return the end value of a temporal JSONB
 * @param[in] temp Temporal value
 * @return On error return @p NULL
 * @csqlfn #Temporal_end_value()
 */
Jsonb *
tjsonb_end_value(const Temporal *temp)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL);
  return DatumGetJsonbP(temporal_end_value(temp));
}

/**
 * @ingroup meos_jsonb_accessor
 * @brief Return the n-th value of a temporal JSONB
 * @param[in] temp Temporal value
 * @param[in] n Number
 * @param[out] result Value
 * @csqlfn #Temporal_value_n()
 */
bool
tjsonb_value_n(const Temporal *temp, int n, Jsonb **result)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, false); VALIDATE_NOT_NULL(result, false);
  Datum dresult;
  if (! temporal_value_n(temp, n, &dresult))
    return false;
  *result = DatumGetJsonbP(dresult);
  return true;
}

/**
 * @ingroup meos_jsonb_accessor
 * @brief Return the array of base values of a temporal JSONB
 * @param[in] temp Temporal value
 * @param[out] count Number of values in the output array
 * @csqlfn #Temporal_valueset()
 */
Jsonb **
tjsonb_values(const Temporal *temp, int *count)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL); VALIDATE_NOT_NULL(count, NULL); 
  Datum *datumarr = temporal_values_p(temp, count);
  Jsonb **result = palloc(sizeof(Jsonb *) * *count);
  for (int i = 0; i < *count; i++)
    result[i] = jsonb_copy(DatumGetJsonbP(datumarr[i]));
  pfree(datumarr);
  return result;
}

/*****************************************************************************/

/**
 * @ingroup meos_jsonb_accessor
 * @brief Return the value of a temporal JSONB at a timestamptz
 * @param[in] temp Temporal value
 * @param[in] t Timestamp
 * @param[in] strict True if the timestamp must belong to the temporal value,
 * false when it may be at an exclusive bound
 * @param[out] value Resulting value
 * @csqlfn #Temporal_value_at_timestamptz()
 */
bool
tjsonb_value_at_timestamptz(const Temporal *temp, TimestampTz t, bool strict,
  Jsonb **value)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, false); VALIDATE_NOT_NULL(value, false);
  Datum res;
  bool result = temporal_value_at_timestamptz(temp, t, strict, &res);
  *value = DatumGetJsonbP(res);
  return result;
}

/*****************************************************************************
 * Transformation functions
 *****************************************************************************/


/*****************************************************************************
 * Restriction functions
 *****************************************************************************/

/**
 * @ingroup meos_jsonb_restrict
 * @brief Return a temporal JSONB restricted to a specific JSONB value
 * @param[in] temp Temporal value
 * @param[in] jsb JSONB value
 * @csqlfn #Temporal_at_value()
 */
Temporal *
tjsonb_at_value(const Temporal *temp, Jsonb *jsb)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL); VALIDATE_NOT_NULL(jsb, NULL);
  /* Restrict the temporal JSONB to the instants where it equals the given jsb */
  return temporal_restrict_value(temp, PointerGetDatum(jsb), REST_AT);
}

/**
 * @ingroup meos_jsonb_restrict
 * @brief Return a temporal JSONB restricted to the complement of a specific
 * JSONB value
 * @param[in] temp Temporal value
 * @param[in] jsb JSONB value
 * @csqlfn #Temporal_minus_value()
 */
Temporal *
tjsonb_minus_value(const Temporal *temp, Jsonb *jsb)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL); VALIDATE_NOT_NULL(jsb, NULL);
  /* Restrict the temporal JSONB to the instants where it does not equal the
   * given jsb */
  return temporal_restrict_value(temp, PointerGetDatum(jsb), REST_MINUS);
}

/*****************************************************************************/

/*****************************************************************************
 * Generic functions on temporal JSONB
 *****************************************************************************/

/**
 * @brief Apply a unary JSONB→JSONB function to each instant of a T_TJSONB.
 */
Temporal *
jsonbfunc_tjsonb(const Temporal *temp, Datum *params, varfunc func,
  meosType intype, meosType paramtype UNUSED, meosType restype)
{
  /* Ensure the validity of the arguments */
  assert(temp); assert(func); assert(temp->temptype == intype);

  LiftedFunctionInfo lfinfo;
  memset(&lfinfo, 0, sizeof(LiftedFunctionInfo));
  lfinfo.func = func;
  lfinfo.numparam = 1;
  lfinfo.param[0] = params[0];
  lfinfo.argtype[0] = intype;
  lfinfo.restype = restype;
  return tfunc_temporal(temp, &lfinfo);
}

/**
 * @brief Apply a binary JSONB→JSONB function between each instant of a
 * T_TJSONB and a constant JSONB.
 */
Temporal *
jsonbfunc_tjsonb_jsonb(const Temporal *temp, Datum value, meosType valuetype,
  datum_func2 func, meosType restype, bool invert)
{
  /* Ensure the validity of the arguments */
  assert(temp); assert(temp->temptype == T_TJSONB);

  LiftedFunctionInfo lfinfo;
  memset(&lfinfo, 0, sizeof(LiftedFunctionInfo));
  lfinfo.func = (varfunc) func;
  lfinfo.numparam = 0;
  lfinfo.argtype[0] = T_TJSONB;
  lfinfo.argtype[1] = valuetype;
  lfinfo.restype = restype;
  lfinfo.reslinear = false;
  lfinfo.invert = invert;
  lfinfo.discont = CONTINUOUS;
  return tfunc_temporal_base(temp, value, &lfinfo);
}

/**
 * @brief Apply a binary JSONB→JSONB function instant-by-instant between two
 *        T_TJSONB values.
 */
Temporal *
jsonbfunc_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2,
  datum_func2 func, meosType restype)
{
  /* Ensure the validity of the arguments */
  assert(temp1); assert(temp2); assert(temp1->temptype == temp2->temptype);
  assert(temp1->temptype == T_TJSONB);

  LiftedFunctionInfo lfinfo;
  memset(&lfinfo, 0, sizeof(LiftedFunctionInfo));
  lfinfo.func = (varfunc) func;
  lfinfo.numparam = 0;
  lfinfo.argtype[0] = lfinfo.argtype[1] = T_TJSONB;
  lfinfo.restype = restype;
  lfinfo.reslinear = false;
  lfinfo.invert = INVERT_NO;
  lfinfo.discont = CONTINUOUS;
  return tfunc_temporal_temporal(temp1, temp2, &lfinfo);
}

Temporal *
jsonbfunc_tjsonb_text(const Temporal *temp, Datum value,
                      datum_func2 func, meosType restype, bool invert)
{
  assert(temp); assert(temp->temptype == T_TJSONB);

  LiftedFunctionInfo lfinfo;
  memset(&lfinfo, 0, sizeof(LiftedFunctionInfo));
  lfinfo.func = (varfunc) func;
  lfinfo.numparam = 0;
  lfinfo.argtype[0] = T_TJSONB;
  lfinfo.argtype[1] = T_TTEXT;  /* second argument is text */
  lfinfo.restype = restype;
  lfinfo.reslinear = false;
  lfinfo.invert = invert;
  lfinfo.discont = CONTINUOUS;

  return tfunc_temporal_base(temp, value, &lfinfo);
}



/*****************************************************************************/

#if MEOS
/**
 * @ingroup meos_jsonb_jsonb
 * @brief Extracts a JSONB object field with the given key
 * @param[in] temp Temporal JSONB value
 * @param[in] key Key
 * @csqlfn #Tjsonb_delete_key()
 */
Temporal *
tjsonb_object_field(const Temporal *temp, const text *key)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL); VALIDATE_NOT_NULL(key, NULL);
  Datum params[1];
  params[0] = PointerGetDatum(key);
  return jsonbfunc_tjsonb(temp, params, (varfunc) &datum_jsonb_object_field,
    T_TJSONB, T_TEXT, T_TJSONB);
}

/**
 * @ingroup meos_jsonb_jsonb
 * @brief Return the concatenation of a JSONB and a temporal JSONB
 * @param[in] jb JSONB value
 * @param[in] temp Temporal JSONB value
 * @csqlfn #Concat_jsonb_tjsonb()
 */
Temporal *
concat_jsonb_tjsonb(const Jsonb *jb, const Temporal *temp)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL); VALIDATE_NOT_NULL(jb, NULL);
  /* concat: func( const, instant ) */
  return jsonbfunc_tjsonb_jsonb(temp, PointerGetDatum(jb), T_TJSONB,
    &datum_jsonb_concat, T_TJSONB, INVERT);
}

/**
 * @ingroup meos_jsonb_jsonb
 * @brief Return the concatenation of a temporal JSONB and a JSONB
 * @param[in] temp Temporal JSONB value
 * @param[in] jb JSONB value
 * @csqlfn #Concat_tjsonb_jsonb()
 */
Temporal *
concat_tjsonb_jsonb(const Temporal *temp, const Jsonb *jb)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL); VALIDATE_NOT_NULL(jb, NULL);
  /* concat: func( instant, const ) */
  return jsonbfunc_tjsonb_jsonb(temp, PointerGetDatum(jb), T_TJSONB,
    &datum_jsonb_concat, T_TJSONB, INVERT_NO);
}

/**
 * @ingroup meos_jsonb_jsonb
 * @brief Return the concatenation of two temporal JSONB values
 * @param[in] temp1, temp2 Temporal JSONB values
 * @csqlfn #Concat_tjsonb_tjsonb()
 */
Temporal *
concat_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp1, NULL); VALIDATE_TJSONB(temp2, NULL);
  return jsonbfunc_tjsonb_tjsonb(temp1, temp2, &datum_jsonb_concat, T_TJSONB);
}
#endif /* MEOS */

/**
 * @ingroup meos_jsonb_jsonb
 * @brief Delete a key from a JSONB value
 * @param[in] temp Temporal JSONB value
 * @param[in] key Key
 * @csqlfn #Tjsonb_delete_key()
 */
Temporal *
tjsonb_delete_key(const Temporal *temp, const text *key)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL); VALIDATE_NOT_NULL(key, NULL);
  Datum params[1];
  params[0] = PointerGetDatum(key);
  return jsonbfunc_tjsonb(temp, params, (varfunc) &datum_jsonb_delete,
    T_TJSONB, T_TEXT, T_TJSONB);
}

/*****************************************************************************/

/**
 * @ingroup meos_jsonb_internal_jsonb
 * @brief Apply a function whose second argument is a text array to a JSONB value
 * @param[in] inst Temporal JSONB instant
 * @param[in] elems Elements
 * @param[in] count Number of elements in the input array
 */
TInstant *
tjsonbinst_func_textarr(const TInstant *inst, const text **elems, bool *nulls,
  int count, Datum *params, JsonbFunc func)
{
  assert(inst); assert(inst->temptype == T_TJSONB);
  switch (func)
  {
    case DELETE_KEY_ARRAY:
    {
      Jsonb *jb = jsonb_delete_key_array_internal(DatumGetJsonbP(
        tinstant_value_p(inst)), elems, nulls, count);
      if (! jb)
        return NULL;
      return tinstant_make(PointerGetDatum(jb), T_TJSONB, inst->t);
    }
    case EXISTS_KEY_ARRAY:
    {
      bool res = jsonb_exists_array(DatumGetJsonbP(tinstant_value_p(inst)),
        elems, count, params);
      return tinstant_make(BoolGetDatum(res), T_TBOOL, inst->t);
    }
    case SET_PATH_ARRAY:
    {
      bool res = jsonb_set_internal(DatumGetJsonbP(tinstant_value_p(inst)),
        (Datum *) elems, nulls, count, DatumGetJsonbP(params[0]),
        DatumGetBool(params[1]));
      return tinstant_make(BoolGetDatum(res), T_TBOOL, inst->t);
    }
    case DELETE_PATH:
    {
      Jsonb *jb = jsonb_delete_path_internal(
        DatumGetJsonbP(tinstant_value_p(inst)), (Datum *) elems, nulls, count);
      return tinstant_make(PointerGetDatum(jb), T_TJSONB, inst->t);
    }
    case INSERT_PATH:
    {
      Jsonb *jb = jsonb_insert_internal(DatumGetJsonbP(tinstant_value_p(inst)),
        (Datum *) elems, nulls, count, DatumGetJsonbP(params[0]),
        DatumGetBool(params[1]));
      return tinstant_make(PointerGetDatum(jb), T_TJSONB, inst->t);
    }
    case EXTRACT_PATH:
    {
      Jsonb *jb = jsonb_extract_path_internal(DatumGetJsonbP(
        tinstant_value_p(inst)), (Datum *) elems, count);
      if (! jb)
        return NULL;
      return tinstant_make(PointerGetDatum(jb), T_TJSONB, inst->t);
    }
    case EXTRACT_PATH_TEXT:
    {
      Jsonb *jb = jsonb_extract_path_text_internal(DatumGetJsonbP(
        tinstant_value_p(inst)), (Datum *) elems, count);
      if (! jb)
        return NULL;
      return tinstant_make(PointerGetDatum(jb), T_TJSONB, inst->t);
    }
    default: /* Error */
    {
      meos_error(ERROR, MEOS_ERR_INTERNAL_ERROR,
        "Unknown JSONB function");
      return NULL;
    }
  }
}

/**
 * @ingroup meos_jsonb_internal_jsonb
 * @brief Apply a function whose second argument is a text array to a JSONB value
 * @param[in] seq Temporal JSONB sequence
 * @param[in] elems Elements
 * @param[in] count Number of elements in the input array
 */
TSequence *
tjsonbseq_func_textarr(const TSequence *seq, const text **elems, bool *nulls,
  int count, Datum *params, JsonbFunc func)
{
  assert(seq); assert(seq->temptype == T_TJSONB);
  TInstant **instants = palloc(sizeof(TInstant) * seq->count);
  int newcount = 0;
  for (int i = 0; i < seq->count; i++)
  {
    TInstant *inst = tjsonbinst_func_textarr(
      (TInstant *) TSEQUENCE_INST_N(seq, i), elems, nulls, count, params,
        func);
    /* If the result is NULL we ignore the instant */
    if (inst)
      instants[newcount++] = inst;
  }
  if (newcount == 0)
  {
    pfree(instants);
    return NULL;
  }
  return tsequence_make_free(instants, newcount, seq->period.lower_inc,
    seq->period.upper_inc, MEOS_FLAGS_GET_INTERP(seq->flags), NORMALIZE);
}

/**
 * @ingroup meos_jsonb_internal_jsonb
 * @brief Apply a function whose second argument is a text array to a JSONB value
 * @param[in] ss Temporal JSONB sequence set
 * @param[in] elems Elements
 * @param[in] count Number of elements in the input array
 */
TSequenceSet *
tjsonbseqset_func_textarr(const TSequenceSet *ss, const text **elems,
  bool *nulls, int count, Datum *params, JsonbFunc func)
{
  assert(ss); assert(ss->temptype == T_TJSONB);
  TSequence **sequences = palloc(sizeof(TSequence) * ss->count);
  int newcount = 0;
  for (int i = 0; i < ss->count; i++)
  {
    TSequence *seq = tjsonbseq_func_textarr(
      (TSequence *) TSEQUENCESET_SEQ_N(ss, i), elems, nulls, count, params,
        func);
    /* If the result is NULL we ignore the instant */
    if (seq)
      sequences[newcount++] = seq;
  }
  if (newcount == 0)
  {
    pfree(sequences);
    return NULL;
  }
  return tsequenceset_make_free(sequences, newcount, NORMALIZE);
}

/**
 * @ingroup meos_jsonb_internal_jsonb
 * @brief Apply a function whose second argument is a text array to a JSONB value
 * @param[in] temp Temporal JSONB value
 * @param[in] elems Elements
 * @param[in] count Number of elements in the input array
 */
Temporal *
tjsonb_func_textarr(const Temporal *temp, const text **elems, bool *nulls,
  int count, Datum *params, JsonbFunc func)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL); VALIDATE_NOT_NULL(elems, NULL);
  assert(count > 0);

  assert(temptype_subtype(temp->subtype));
  switch (temp->subtype)
  {
    case TINSTANT:
      return (Temporal *) tjsonbinst_func_textarr((TInstant *) temp,
        elems, nulls, count, params, func);
    case TSEQUENCE:
      return (Temporal *) tjsonbseq_func_textarr((TSequence *) temp,
        elems, nulls, count, params, func);
    default: /* TSEQUENCESET */
      return (Temporal *) tjsonbseqset_func_textarr((TSequenceSet *) temp,
        elems, nulls, count, params, func);
  }
}

/*****************************************************************************
 * JSONB → TFloat conversion
 *****************************************************************************/

/**
 * @brief Convert a temporal JSONB to a temporal float by extracting one key
 * @param[in] temp Temporal JSONB object
 * @param[in] key  Key to extract
 * @return New temporal float with values extracted from JSONB
 */
Temporal *
tjsonb_to_tfloat_internal(const Temporal *temp, const char *key)
{
  VALIDATE_TJSONB(temp, NULL);

  switch (temp->subtype)
  {
    case TINSTANT:
      return (Temporal *) tjsonbinst_to_tfloatinst((const TInstant *) temp, key);
    case TSEQUENCE:
      return (Temporal *) tjsonbseq_to_tfloatseq((const TSequence *) temp, key);
    case TSEQUENCESET:
      return (Temporal *) tjsonbseqset_to_tfloatseqset((const TSequenceSet *) temp, key);
    default:
      meos_error(ERROR, MEOS_ERR_INTERNAL_ERROR,
        "Unsupported temporal subtype in tjsonb_to_tfloat");
  }
  return NULL; /* never reached */
}

/**
 * @brief Convert a temporal instant containing a JSONB value into a TFloat instant
 * @param[in] inst Temporal instant containing a JSONB value
 * @param[in] key  Key to extract from the JSONB object
 * @return A new temporal float instant with the extracted value
 * @pre The input instant must contain a JSONB value
 * @note Supported JSONB types: numeric, boolean, string
 */
TInstant *
tjsonbinst_to_tfloatinst(const TInstant *inst, const char *key)
{
  Datum dvalue = tinstant_value_p(inst);
  Jsonb *jb = DatumGetJsonbP(dvalue);

  /* Lookup key in the JSONB object */
  JsonbValue k, *v;
  k.type = jbvString;
  k.val.string.len = strlen(key);
  k.val.string.val = (char *) key;

  v = findJsonbValueFromContainer(&jb->root, JB_FOBJECT, &k);
  if (v == NULL)
    meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
      "JSON key %s not found", key);
  double val = 0.0;
  
  switch (v->type)
  {
    case jbvNumeric:
    {
      /* Convert Numeric → C string → double */
      char *cstr = numeric_out_internal(v->val.numeric);
      char *endptr;
      val = strtod(cstr, &endptr);
      pfree(cstr);
      if (endptr == cstr)
        meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
          "Invalid numeric value for key %s", key);
      break;
    }
    case jbvBool:
      val = v->val.boolean ? 1.0 : 0.0;
      break;
    case jbvString:
    {
      char *buf = palloc(v->val.string.len + 1);
      memcpy(buf, v->val.string.val, v->val.string.len);
      buf[v->val.string.len] = '\0';
      char *endptr;
      val = strtod(buf, &endptr);
      if (endptr == buf)
        meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
          "Invalid numeric string for key %s", key);
      break;
    }
    default:
      meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
        "Unsupported JSONB value type for key %s", key);
  }
  /* Build and return a new float instant with the same timestamp */
  return tinstant_make(Float8GetDatum(val), T_TFLOAT, inst->t);
}

/**
 * @brief Convert a temporal sequence of JSONB instants into a TFloat sequence
 * @param[in] seq Temporal sequence of JSONB instants
 * @param[in] key Key to extract from each JSONB object
 * @return A new temporal float sequence with the same period and interpolation
 * @pre The input sequence must contain JSONB instants
 */
TSequence *
tjsonbseq_to_tfloatseq(const TSequence *seq, const char *key)
{
  /* Allocate an array of float instants, one per JSONB instant in seq */
  TInstant **instants = palloc(sizeof(TInstant *) * seq->count);
  /* Convert each JSONB instant into a float instant */
  for (int i = 0; i < seq->count; i++)
    instants[i] = tjsonbinst_to_tfloatinst(TSEQUENCE_INST_N(seq, i), key);
/* Build a new float sequence with the same temporal properties */
  return tsequence_make((const TInstant **) instants, seq->count,
    seq->period.lower_inc, seq->period.upper_inc, LINEAR, NORMALIZE);
}

/**
 * @brief Convert a temporal sequence set of JSONB instants into a TFloat sequence set
 * @param[in] ss Temporal sequence set of JSONB instants
 * @param[in] key Key to extract from each JSONB object
 * @return A new temporal float sequence set
 * @pre The input must contain JSONB instants
 */
TSequenceSet *
tjsonbseqset_to_tfloatseqset(const TSequenceSet *ss, const char *key)
{
  /* Allocate an array of float sequences, one per JSONB sequence */
  TSequence **sequences = palloc(sizeof(TSequence *) * ss->count);
  /* Convert each JSONB sequence into a float sequence */
  for (int i = 0; i < ss->count; i++)
    sequences[i] = tjsonbseq_to_tfloatseq(TSEQUENCESET_SEQ_N(ss, i), key);
  /* Build a new float sequence set */
  return tsequenceset_make((const TSequence **) sequences, ss->count, NORMALIZE);
}

/**
 * @brief Compute the time-weighted average of a temporal JSONB value
 * This internal function converts the temporal JSONB to a temporal float
 * using @ref tjsonb_to_tfloat_internal and delegates the computation to
 * the existing @ref tnumber_twavg.
 * @param[in] temp Temporal JSONB object
 * @param[in] key  Key to extract from each JSONB object
 * @return Time-weighted average of the extracted values, or DBL_MAX on error
 * @see Tjsonb_twavg
 */
double
tjsonb_twavg_internal(const Temporal *temp, const char *key)
{
  VALIDATE_TJSONB(temp, DBL_MAX);
  /* Convert JSONB → tfloat */
  Temporal *t = tjsonb_to_tfloat_internal(temp, key);
  /* Use the existing implementation for temporal numbers */
  double result = tnumber_twavg(t);
  pfree(t);
  return result;
}

/*****************************************************************************/

/**
 * @brief Extract a double value from a temporal instant containing JSONB
 * @param[in] inst Temporal instant with a JSONB value
 * @param[in] key  Key to extract from the JSONB object
 * @return The extracted value converted to double
 * @pre The input instant must contain a JSONB value
 * @note Supported JSONB types are numeric, boolean, and string.
 *       An error is raised if the key is missing or the value type is unsupported.
 */
double
tjsonbinst_double(const TInstant *inst, const char *key)
{
  Datum dvalue = tinstant_value_p(inst);
  Jsonb *jb = DatumGetJsonbP(dvalue);

  JsonbValue k, *v;
  k.type = jbvString;
  k.val.string.len = strlen(key);
  k.val.string.val = (char *) key;

  v = findJsonbValueFromContainer(&jb->root, JB_FOBJECT, &k);
  if (v == NULL)
    ereport(ERROR,
      (errmsg("JSON key %s not found", key)));

  switch (v->type)
  {
    case jbvNumeric:
    {
      /* Convert numeric to string then to double */
      char *cstr = DatumGetCString(DirectFunctionCall1(numeric_out,
        NumericGetDatum(v->val.numeric)));
      char *endptr;
      double val = strtod(cstr, &endptr);
      pfree(cstr);

      if (endptr == cstr)
        ereport(ERROR,
          (errmsg("Invalid numeric value for key %s", key)));

      return val;
    }

    case jbvBool:
      return v->val.boolean ? 1.0 : 0.0;

    case jbvString:
    {
      char *buf = palloc(v->val.string.len + 1);
      memcpy(buf, v->val.string.val, v->val.string.len);
      buf[v->val.string.len] = '\0';

      char *endptr;
      double val = strtod(buf, &endptr);
      if (endptr == buf)
        ereport(ERROR,
          (errmsg("Invalid numeric string for key %s", key)));

      return val;
    }

    default:
      ereport(ERROR,
        (errmsg("Unsupported JSONB value type for key %s", key)));
  }

  return DBL_MAX; /* should never reach */
}

/**
 * @brief Compute the time-weighted average of a temporal JSONB sequence
 * @param[in] seq Temporal sequence of JSONB instants
 * @param[in] key Key to extract from each JSONB object
 * @return Time-weighted average value, or DBL_MAX if duration is zero
 * @pre The input sequence must contain JSONB instants
 */
double
tjsonbseq_twavg(const TSequence *seq, const char *key)
{
  assert(seq);
  assert(seq->temptype == T_TJSONB);

  if (seq->count == 1)
    return tjsonbinst_double((TInstant *) TSEQUENCE_INST_N(seq, 0), key);

  double sum = 0.0;
  double duration = 0.0;

  for (int i = 0; i < seq->count - 1; i++)
  {
    const TInstant *inst1 = TSEQUENCE_INST_N(seq, i);
    const TInstant *inst2 = TSEQUENCE_INST_N(seq, i+1);

    double val1 = tjsonbinst_double(inst1, key);
    double val2 = tjsonbinst_double(inst2, key);

    double segdur = (double)(inst2->t - inst1->t); /* microseconds */
    double avgval = (val1 + val2) / 2.0;

    sum += avgval * segdur;
    duration += segdur;
  }

  return (duration > 0.0) ? sum / duration : DBL_MAX;
}

/**
 * @brief Compute the time-weighted average of a temporal JSONB sequence set
 * @param[in] ss Temporal sequence set of JSONB instants
 * @param[in] key Key to extract from each JSONB object
 * @return Time-weighted average value, or DBL_MAX if duration is zero
 * @pre The input sequence set must contain JSONB instants
 */
double
tjsonbseqset_twavg(const TSequenceSet *ss, const char *key)
{
  assert(ss);
  assert(ss->temptype == T_TJSONB);

  double sum = 0.0;
  double duration = 0.0;

  for (int i = 0; i < ss->count; i++)
  {
    const TSequence *seq = TSEQUENCESET_SEQ_N(ss, i);

    double seq_avg = tjsonbseq_twavg(seq, key);
    double seq_dur = (double)(DatumGetTimestampTz(seq->period.upper) -
                              DatumGetTimestampTz(seq->period.lower));

    if (seq_avg != DBL_MAX)
    {
      sum += seq_avg * seq_dur;
      duration += seq_dur;
    }
  }

  return (duration > 0.0) ? sum / duration : DBL_MAX;
}

/*****************************************************************************/

