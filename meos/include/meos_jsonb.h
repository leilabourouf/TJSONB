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
 * @brief API of the Mobility Engine Open Source (MEOS) library.
 */

#ifndef __MEOS_JSONB_H__
#define __MEOS_JSONB_H__

/* C */
#include <stdbool.h>
#include <stdint.h>
/* JSON-C */
#include <json-c/json.h>
/* PostgreSQL */
#include <utils/jsonb.h>
/* MEOS */
#include <meos.h>

/*****************************************************************************
 * Type definitions
 *****************************************************************************/

/* Opaque structure to represent JSONBs */

typedef struct Cbuffer Cbuffer;

/*****************************************************************************
 * Validity macros and functions
 *****************************************************************************/

/**
 * @brief Macro for ensuring that a set is a JSONB set
 */
#if MEOS
  #define VALIDATE_JSONBSET(set, ret) \
    do { \
      if (! ensure_not_null((void *) (set)) || \
          ! ensure_set_isof_type((set), T_JSONBSET) ) \
        return (ret); \
    } while (0)
//jsnb
#else
  #define VALIDATE_JSONBSET(set, ret) \
    do { \
      assert(set); \
      assert((set)->settype == T_JSONBSET); \
    } while (0)
#endif /* MEOS */

/**
 * @brief Macro for ensuring that the temporal value is a temporal JSONB
 * @note The macro works for the Temporal type and its subtypes TInstant,
 * TSequence, and TSequenceSet
 */
#if MEOS
  #define VALIDATE_TJSONB(temp, ret) \
    do { \
      if (! ensure_not_null((void *) (temp)) || \
          ! ensure_temporal_isof_type((Temporal *) (temp), T_TJSONB) ) \
        return (ret); \
    } while (0)
#else
  #define VALIDATE_TJSONB(temp, ret) \
    do { \
      assert(temp); \
      assert(((Temporal *) (temp))->temptype == T_TJSONB); \
    } while (0)
#endif /* MEOS */

/******************************************************************************
 * Functions for JSONB
 ******************************************************************************/

/* Input and output functions */

extern char *meos_jsonb_out(const Jsonb *jb);

/* Constructor functions */

extern Jsonb *jsonb_copy(const Jsonb *jb);

/* Conversion functions */

extern Jsonb *cstring2jsonb(const char *str);
extern char  *jsonb2cstring(const Jsonb *jb);

/* Accessor functions */

/* Transformation functions */

/* Bounding box functions */

/* Distance functions */

/* Comparison functions */

extern int jsonb_cmp_internal(const Jsonb *jb1, const Jsonb *jb2);

/******************************************************************************
 * Functions for JSONB sets
 ******************************************************************************/

/* Input and output functions */

extern Set * jsonbset_in(const char *str);
extern char * jsonbset_out(const Set *set);

/* Constructor functions */

// extern Set *cbufferset_make(const Cbuffer **values, int count);

/* Conversion functions */

extern Set * jsonb_to_set(const Jsonb *jsonb);

/* Accessor functions */

extern Jsonb *jsonbset_start_value(const Set *s);
extern Jsonb *jsonbset_end_value(const Set *s);
extern bool jsonbset_value_n(const Set *s, int n, Jsonb **result);
extern Jsonb **jsonbset_values(const Set *s);

/* Transformation functions */

// extern Set * jsonbcat_jsonb_jsonbset_int(const Set *s, const Jsonb *jb, bool invert);

/* Set operations */

extern Set * jsonbcat_jsonb_jsonbset(const Jsonb *jb, const Set *s);
extern Set * jsonbcat_jsonbset_jsonb(const Set *s, const Jsonb *jb);
extern bool contained_jsonb_set(const Jsonb *jsonb, const Set *s);
extern bool contains_set_jsonb(const Set *s,  Jsonb *jsonb);
extern Set * intersection_jsonb_set(const Jsonb *jb, const Set *s);
extern Set * intersection_set_jsonb(const Set *s, const Jsonb *jb);
extern Set * minus_set_jsonb(const Set *s, const Jsonb *jb);
extern Set * minus_jsonb_set(const Jsonb *jb, const Set *s);
extern Set * union_set_jsonb(const Set *s, const Jsonb *jb);
extern Set *union_jsonb_set(const Jsonb *jb, const Set *s);

/* Aggregate functions for set and span types */

extern Set * jsonb_union_transfn(Set *state, const Jsonb *jsonb);

/*===========================================================================*
 * Functions for temporal types
 *===========================================================================*/

/*****************************************************************************
 * Input/output functions
 *****************************************************************************/

extern Temporal * tjsonb_from_mfjson(const char *str);
extern void jsonb_as_mfjson_sb(stringbuffer_t *sb, const Jsonb *jb);
extern Temporal * tjsonb_in(const char *str);
extern char * tjsonb_out(const Temporal *temp);

extern TInstant *tjsonbinst_from_mfjson(json_object *mfjson);
extern TInstant *tjsonbinst_in(const char *str);
extern TSequence *tjsonbseq_from_mfjson(json_object *mfjson);
extern TSequence *tjsonbseq_in(const char *str, interpType interp);
extern TSequenceSet *tjsonbseqset_from_mfjson(json_object *mfjson);
extern TSequenceSet *tjsonbseqset_in(const char *str);

/*****************************************************************************
 * Constructor functions
 *****************************************************************************/

extern Temporal * tjsonb_from_base_temp(const Jsonb *jsonb, const Temporal *temp);
extern TInstant * tjsonbinst_make(const Jsonb *jsonb, TimestampTz t);
extern TSequence * tjsonbseq_from_base_tstzset(const Jsonb *jsonb, const Set *s);
extern TSequence * tjsonbseq_from_base_tstzspan(const Jsonb *jsonb, const Span *s);
extern TSequenceSet * tjsonbseqset_from_base_tstzspanset(const Jsonb *jsonb, const SpanSet *ss);

/*****************************************************************************
 * Accessor functions
 *****************************************************************************/

extern Jsonb * tjsonb_end_value(const Temporal *temp);
extern Jsonb * tjsonb_start_value(const Temporal *temp);
extern bool tjsonb_value_at_timestamptz(const Temporal *temp, TimestampTz t, bool strict,  Jsonb **value);
extern bool tjsonb_value_n(const Temporal *temp,   int n, Jsonb **result);
extern Jsonb ** tjsonb_values(const Temporal *temp,  int *count);

/*****************************************************************************
 * Conversion functions
 *****************************************************************************/


/*****************************************************************************
 * Transformation functions
 *****************************************************************************/


/*****************************************************************************
 * Restriction functions
 *****************************************************************************/

extern Temporal * tjsonb_at_value(const Temporal *temp, Jsonb *jsb);
extern Temporal * tjsonb_minus_value(const Temporal *temp,  Jsonb *jsb);

/*****************************************************************************
 * Distance functions
 *****************************************************************************/


/*****************************************************************************
 * Comparison functions
 *****************************************************************************/

/* Ever/always comparison functions */

extern int always_eq_jsonb_tjsonb(const Jsonb *jsonb, const Temporal *temp);
extern int always_eq_tjsonb_jsonb(const Temporal *temp, const Jsonb *jsonb);
extern int always_eq_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2);
extern int always_ne_jsonb_tjsonb(const Jsonb *jsonb, const Temporal *temp);
extern int always_ne_tjsonb_jsonb(const Temporal *temp, const Jsonb *jsonb);
extern int always_ne_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2);
extern int ever_eq_jsonb_tjsonb(const Jsonb *jsonb, const Temporal *temp);
extern int ever_eq_tjsonb_jsonb(const Temporal *temp, const Jsonb *jsonb);
extern int ever_eq_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2);
extern int ever_ne_jsonb_tjsonb(const Jsonb *jsonb, const Temporal *temp);
extern int ever_ne_tjsonb_jsonb(const Temporal *temp, const Jsonb *jsonb);
extern int ever_ne_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2);

/* Temporal comparison functions */

extern Temporal *teq_jsonb_tjsonb(const Jsonb *jsonb, const Temporal *temp);
extern Temporal *teq_tjsonb_jsonb(const Temporal *temp, const Jsonb *jsonb);
extern Temporal *teq_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2);
extern Temporal *tne_jsonb_tjsonb(const Jsonb *jsonb, const Temporal *temp);
extern Temporal *tne_tjsonb_jsonb(const Temporal *temp, const Jsonb *jsonb);
extern Temporal *tne_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2);

/*****************************************************************************
 * Temporal JSONB functions
 *****************************************************************************/

extern Jsonb * concat_jsonb_jsonb(const Jsonb *jb1, const Jsonb *jb2); 
extern Temporal * concat_jsonb_tjsonb(const Jsonb *jb, const Temporal *temp);
extern Temporal * concat_tjsonb_jsonb(const Temporal *temp, const Jsonb *jb);
extern Temporal * concat_tjsonb_tjsonb(const Temporal *temp1, const Temporal *temp2);
extern Temporal * jsonb_set_tjsonb_jsonb(const Temporal *temp, const Jsonb *jb);
extern Temporal * jsonb_set_jsonb_tjsonb(const Jsonb *value, const Temporal *temp); 

/*****************************************************************************/

#endif /* __MEOS_JSONB_H__ */
