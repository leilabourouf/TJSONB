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
#include <limits.h>
/* PostgreSQL */
#include <postgres.h>
#include "utils/jsonb.h"
/* PostGIS */
#include <liblwgeom_internal.h>
/* MEOS */
#include <meos.h>
#include <meos_jsonb.h>
#include <meos_internal_geo.h>
#include "temporal/temporal.h"
#include "temporal/lifting.h"
#include "temporal/type_util.h"

/*****************************************************************************
 * Input/output functions
 *****************************************************************************/

#if MEOS
/**
 * @ingroup meos_jsonb_inout
 * @brief Return a temporal JSONB from its Well-Known Text (WKT) representation
 * @param[in] str String
 * @csqlfn #Tjsonb_in()
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
 * @csqlfunc #Tjsonbinst_in()
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
 * @csqlfunc #Tjsonbseq_in()
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
 * @csqlfunc #Tjsonbseqset_in()
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

/**
 * @ingroup meos_jsonb_inout
 * @brief Return the Well-Known Text (WKT) representation of a temporal JSONB
 * @param[in] temp Temporal JSONB
 * @csqlfn #TJSONB_out()
 */
char *
tjsonb_out(const Temporal *temp)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL);
  return temporal_out(temp, 0);
}

/*****************************************************************************/

/**
 * @ingroup meos_internal_temporal_inout
 * @brief Return a temporal JSONB instant from its MF-JSON representation
 * @param[in] mfjson MFJSON object
 * @csqlfn #Temporal_from_mfjson()
 */
inline TInstant *
tjsonbinst_from_mfjson(json_object *mfjson)
{
  /* false = not linear, 0 = unused */
  return tinstant_from_mfjson(mfjson, false, 0, T_TJSONB);
}

/**
 * @ingroup meos_internal_temporal_inout
 * @brief Return a temporal JSONB sequence from its MF-JSON representation
 * @param[in] mfjson MFJSON object
 * @csqlfn #Temporal_from_mfjson()
 */
inline TSequence *
tjsonbseq_from_mfjson(json_object *mfjson)
{
  /* false = not linear, 0 = unused */
  return tsequence_from_mfjson(mfjson, false, 0, T_TJSONB, STEP);
}

/**
 * @ingroup meos_internal_temporal_inout
 * @brief Return a temporal JSONB sequence set from its MF-JSON representation
 * @param[in] mfjson MFJSON object
 * @csqlfn #Temporal_from_mfjson()
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
 * @ingroup meos_internal_jsonb_constructor
 * @brief Return a temporal JSONB instant from a JSONB and a timestamptz
 * @param[in] jsonb Value
 * @param[in] t Timestamp
 * @csqlfn #TJSONBINST_constructor()
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
 * @csqlfunc #TJSONBSEQSET_from_base_tstzspanset()
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
 * @csqlfn #TJSONB_from_base_temp()
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
jsonbfunc_tjsonb(const Temporal *temp, datum_func1 func)
{
  /* Ensure the validity of the arguments */
  assert(temp); assert(func); assert(temp->temptype == T_TJSONB);

  LiftedFunctionInfo lfinfo;
  memset(&lfinfo, 0, sizeof(LiftedFunctionInfo));
  lfinfo.func = (varfunc) func;
  lfinfo.numparam = 0;
  lfinfo.argtype[0] = T_TJSONB;
  lfinfo.restype = T_TJSONB;
  return tfunc_temporal(temp, &lfinfo);
}

/**
 * @brief Apply a binary JSONB→JSONB function between each instant of a
 * T_TJSONB and a constant JSONB.
 */
Temporal *
jsonbfunc_tjsonb_text(const Temporal *temp, const text *value,
  datum_func2 func, bool invert)
{
  /* Ensure the validity of the arguments */
  assert(temp); assert(temp->temptype == T_TJSONB);

  LiftedFunctionInfo lfinfo;
  memset(&lfinfo, 0, sizeof(LiftedFunctionInfo));
  lfinfo.func = (varfunc) func;
  lfinfo.numparam = 0;
  lfinfo.argtype[0] = T_TJSONB;
  lfinfo.argtype[1] = T_TEXT;
  lfinfo.restype = T_TJSONB;
  lfinfo.reslinear = false;
  lfinfo.invert = invert;
  lfinfo.discont = CONTINUOUS;
  return tfunc_temporal_base(temp, PointerGetDatum(value), &lfinfo);
}

/**
 * @brief Apply a binary JSONB→JSONB function between each instant of a
 * T_TJSONB and a constant JSONB.
 */
Temporal *
jsonbfunc_tjsonb_jsonb(const Temporal *temp, Datum value, datum_func2 func,
  bool invert)
{
  /* Ensure the validity of the arguments */
  assert(temp); assert(temp->temptype == T_TJSONB);

  LiftedFunctionInfo lfinfo;
  memset(&lfinfo, 0, sizeof(LiftedFunctionInfo));
  lfinfo.func = (varfunc) func;
  lfinfo.numparam = 0;
  lfinfo.argtype[0] = T_TJSONB;
  lfinfo.argtype[1] = T_JSONB;
  lfinfo.restype = T_TJSONB;
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
  datum_func2 func)
{
  /* Ensure the validity of the arguments */
  assert(temp1); assert(temp2); assert(temp1->temptype == temp2->temptype);
  assert(temp1->temptype == T_TJSONB);

  LiftedFunctionInfo lfinfo;
  memset(&lfinfo, 0, sizeof(LiftedFunctionInfo));
  lfinfo.func = (varfunc) func;
  lfinfo.numparam = 0;
  lfinfo.argtype[0] = lfinfo.argtype[1] = T_TJSONB;
  lfinfo.restype = T_TJSONB;
  lfinfo.reslinear = false;
  lfinfo.invert = INVERT_NO;
  lfinfo.discont = CONTINUOUS;
  return tfunc_temporal_temporal(temp1, temp2, &lfinfo);
}

/*****************************************************************************/

#if MEOS
/**
 * @ingroup meos_temporal_jsonb
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
  return jsonbfunc_tjsonb_jsonb(temp, PointerGetDatum(jb),
    &datum_jsonb_concat, INVERT);
}

/**
 * @ingroup meos_temporal_jsonb
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
  return jsonbfunc_tjsonb_jsonb(temp, PointerGetDatum(jb),
    &datum_jsonb_concat, INVERT_NO);
}

/**
 * @ingroup meos_temporal_jsonb
 * @brief Return the concatenation of two temporal JSONB values
 * @param[in] temp1, temp2 Temporal JSONB values
 * @csqlfn #Concat_tjsonb_tjsonb()
 */
Temporal *
concat_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp1, NULL); VALIDATE_TJSONB(temp2, NULL);
  return jsonbfunc_tjsonb_tjsonb(temp1, temp2, &datum_jsonb_concat);
}

/**
 * @ingroup meos_temporal_jsonb
 * @brief Delete a key from a JSONB value
 * @param[in] temp Temporal JSONB value
 * @param[in] key Key
 * @csqlfn #Delete_jsonb_tjsonb()
 */
Temporal *
delete_tjsonb_key(const Temporal *temp, const text *key)
{
  /* Ensure the validity of the arguments */
  VALIDATE_TJSONB(temp, NULL); VALIDATE_NOT_NULL(key, NULL);
  /* delete: func( const, instant ) */
  return jsonbfunc_tjsonb_text(temp, key, &datum_jsonb_delete, INVERT);
}
#endif /* MEOS */

/*****************************************************************************/
