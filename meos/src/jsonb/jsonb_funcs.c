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
#include <string.h>
#include <stdlib.h>
/* PostgreSQL */
#include <postgres.h>
#include <common/hashfn.h>
#include <common/int.h>
#include <utils/jsonb.h>
/* MEOS */
#include <meos.h>
#include <meos_jsonb.h>
#include "temporal/postgres_types.h"
#include "temporal/temporal.h"
#include "temporal/lifting.h"
#include "temporal/type_util.h"
#include "jsonb/tjsonb_funcs.h"

/* TODO REMOVE TO AVOID CALLING POSTGRESQL FUNCTIONS DIRECTLY */
#if ! MEOS
extern Datum jsonb_in(PG_FUNCTION_ARGS);
#endif /* ! MEOS */

extern int strtoint(const char *pg_restrict str, char **pg_restrict endptr, int base);

/*****************************************************************************
 * JSONB functions
 *****************************************************************************/

/**
 * @ingroup meos_base_types
 * @brief Copy a JSONB value
 * @param[in] jb Jsonb
 */
Jsonb *
jsonb_copy(const Jsonb *jb)
{
  assert(jb);
  Jsonb *result = palloc(VARSIZE(jb));
  memcpy(result, jb, VARSIZE(jb));
  return result;
}

/*****************************************************************************/

/**
 * @ingroup meos_jsonb_base_comp
 * @brief Return the concatenation of the two JSONB values (objects ou arrays)
 */
JsonbValue *
IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
  JsonbParseState **state)
{
  JsonbValue v1, v2, *res = NULL;
  JsonbIteratorToken r1, r2, rk1, rk2;

  rk1 = JsonbIteratorNext(it1, &v1, false);
  rk2 = JsonbIteratorNext(it2, &v2, false);

  /*
   * JsonbIteratorNext reports raw scalars as if they were single-element
   * arrays; hence we only need consider "object" and "array" cases here.
   */
  if (rk1 == WJB_BEGIN_OBJECT && rk2 == WJB_BEGIN_OBJECT)
  {
    /*
     * Both inputs are objects.
     *
     * Append all the tokens from v1 to res, except last WJB_END_OBJECT
     * (because res will not be finished yet).
     */
    pushJsonbValue(state, rk1, NULL);
    while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_OBJECT)
      pushJsonbValue(state, r1, &v1);

    /*
     * Append all the tokens from v2 to res, including last WJB_END_OBJECT
     * (the concatenation will be completed).  Any duplicate keys will
     * automatically override the value from the first object.
     */
    while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
      res = pushJsonbValue(state, r2, r2 != WJB_END_OBJECT ? &v2 : NULL);
  }
  else if (rk1 == WJB_BEGIN_ARRAY && rk2 == WJB_BEGIN_ARRAY)
  {
    /*
     * Both inputs are arrays.
     */
    pushJsonbValue(state, rk1, NULL);

    while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_ARRAY)
    {
      Assert(r1 == WJB_ELEM);
      pushJsonbValue(state, r1, &v1);
    }

    while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_END_ARRAY)
    {
      Assert(r2 == WJB_ELEM);
      pushJsonbValue(state, WJB_ELEM, &v2);
    }

    res = pushJsonbValue(state, WJB_END_ARRAY, NULL /* signal to sort */ );
  }
  else if (rk1 == WJB_BEGIN_OBJECT)
  {
    /*
     * We have object || array.
     */
    Assert(rk2 == WJB_BEGIN_ARRAY);

    pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);

    pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
    while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_DONE)
      pushJsonbValue(state, r1, r1 != WJB_END_OBJECT ? &v1 : NULL);

    while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
      res = pushJsonbValue(state, r2, r2 != WJB_END_ARRAY ? &v2 : NULL);
  }
  else
  {
    /*
     * We have array || object.
     */
    Assert(rk1 == WJB_BEGIN_ARRAY);
    Assert(rk2 == WJB_BEGIN_OBJECT);

    pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);

    while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_ARRAY)
      pushJsonbValue(state, r1, &v1);

    pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
    while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
      pushJsonbValue(state, r2, r2 != WJB_END_OBJECT ? &v2 : NULL);

    res = pushJsonbValue(state, WJB_END_ARRAY, NULL);
  }

  return res;
}

/**
 * @brief Return the concatenation of the two JSONB values (objects ou arrays)
 * @note Derived from the PostgreSQL function @p jsonb_concat(PG_FUNCTION_ARGS)
 */
Jsonb *
concat_jsonb_jsonb(const Jsonb *jb1, const Jsonb *jb2)
{
  /*
   * If one of the jsonb is empty, just return the other if it's not scalar
   * and both are of the same kind.  If it's a scalar or they are of
   * different kinds we need to perform the concatenation even if one is
   * empty.
   */
  if (JB_ROOT_IS_OBJECT(jb1) == JB_ROOT_IS_OBJECT(jb2))
  {
    if (JB_ROOT_COUNT(jb1) == 0 && !JB_ROOT_IS_SCALAR(jb2))
      return jsonb_copy(jb2);
    else if (JB_ROOT_COUNT(jb2) == 0 && !JB_ROOT_IS_SCALAR(jb1))
      return jsonb_copy(jb1);
  }

  JsonbIterator *it1 = JsonbIteratorInit(&((Jsonb *) jb1)->root);
  JsonbIterator *it2 = JsonbIteratorInit(&((Jsonb *) jb2)->root);
  JsonbParseState *state = NULL;
  JsonbValue *res = IteratorConcat(&it1, &it2, &state);
  assert(res != NULL);

  return JsonbValueToJsonb(res);
}

/**
 * @brief Return the concatenation of the two JSONB values (objects ou arrays)
 */
Datum
datum_jsonb_concat(Datum l, Datum r)
{
  return PointerGetDatum(concat_jsonb_jsonb(DatumGetJsonbP(l),
    DatumGetJsonbP(r)));
}

/**
 * @brief Return a copy of the jsonb value with the indicated items removed
 * @note Derived from the PostgreSQL function @p jsonb_delete (jsonb, text)
 */
Jsonb *
jsonb_delete_internal(const Jsonb *in, const text *key)
{
  char *keyptr = VARDATA_ANY(key);
  int keylen = VARSIZE_ANY_EXHDR(key);
  JsonbParseState *state = NULL;
  JsonbValue v, *res = NULL;
  bool skipNested = false;

  if (JB_ROOT_IS_SCALAR(in))
  {
    meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE, "cannot delete from scalar");
    return NULL;
  }

  if (JB_ROOT_COUNT(in) == 0)
    return jsonb_copy(in);

  JsonbIterator *it = JsonbIteratorInit(&((Jsonb *) in)->root);
  JsonbIteratorToken r;
  while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
  {
    skipNested = true;
    if ((r == WJB_ELEM || r == WJB_KEY) &&
      (v.type == jbvString && keylen == v.val.string.len &&
       memcmp(keyptr, v.val.string.val, keylen) == 0))
    {
      /* skip corresponding value as well */
      if (r == WJB_KEY)
        (void) JsonbIteratorNext(&it, &v, true);
      continue;
    }
    res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
  }

  assert(res != NULL);
  return JsonbValueToJsonb(res);
}

/**
 * @brief Return a copy of the JSONB value with the indicated item removed
 */
Datum
datum_jsonb_delete(Datum l, Datum r)
{
  return PointerGetDatum(jsonb_delete_internal(DatumGetJsonbP(l),
    DatumGetTextP(r)));
}

/**
 * @brief Return a copy of the JSONB with the indicated items removed
 * @note Derived from PostgreSQL function @p jsonb_delete_array()
 */
Jsonb *
jsonb_delete_key_array_internal(const Jsonb *in, const text **keys_elems,
  bool *keys_nulls, int keys_len)
{
  JsonbParseState *state = NULL;
  JsonbIterator *it;
  JsonbValue v, *res = NULL;
  bool skipNested = false;
  JsonbIteratorToken r;

  if (JB_ROOT_IS_SCALAR(in))
  {
    meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
      "cannot delete from scalar");
    return NULL;
  }

  if (JB_ROOT_COUNT(in) == 0 || keys_len == 0)
    return jsonb_copy(in);

  it = JsonbIteratorInit(&((Jsonb *) in)->root);
  while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
  {
    skipNested = true;
    if ((r == WJB_ELEM || r == WJB_KEY) && v.type == jbvString)
    {
      int i;
      bool found = false;
      for (i = 0; i < keys_len; i++)
      {
        char *keyptr;
        int keylen;

        if (keys_nulls[i])
          continue;

        /* We rely on the array elements not being toasted */
        keyptr = VARDATA_ANY(keys_elems[i]);
        keylen = VARSIZE_ANY_EXHDR(keys_elems[i]);
        if (keylen == v.val.string.len &&
          memcmp(keyptr, v.val.string.val, keylen) == 0)
        {
          found = true;
          break;
        }
      }
      if (found)
      {
        /* skip corresponding value as well */
        if (r == WJB_KEY)
          (void) JsonbIteratorNext(&it, &v, true);
        continue;
      }
    }
    res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
  }

  assert(res != NULL);
  return JsonbValueToJsonb(res);
}

/**
 * @brief Return the absolute value of a signed 32-bit integer as uint32
 */
static inline uint32
pg_abs_s32(int32 a)
{
  /*
   * Widen to int64, take abs with llabs(), then cast back to uint32.
   * This avoids overflow when a = PG_INT32_MIN.
   */
  return (uint32) llabs((int64) a);
}

/**
 * @brief Return a copy of the jsonb with the indicated item removed
 * @details Negative int means count back from the end of the items
 * @note Derived from PostgreSQL function @p jsonb_delete_idx(jsonb, int)
 */
Jsonb *
jsonb_delete_idx_internal(Jsonb *in, int idx)
{
  JsonbParseState *state = NULL;
  JsonbIterator *it;
  uint32 i = 0, n;
  JsonbValue v, *res = NULL;
  JsonbIteratorToken r;

  if (JB_ROOT_IS_SCALAR(in))
    meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
         "cannot delete from scalar");

  if (JB_ROOT_IS_OBJECT(in))
    meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
      "cannot delete from object using integer index");

  if (JB_ROOT_COUNT(in) == 0)
    return jsonb_copy(in);

  it = JsonbIteratorInit(&in->root);
  r = JsonbIteratorNext(&it, &v, false);
  assert(r == WJB_BEGIN_ARRAY);
  n = v.val.array.nElems;

  if (idx < 0)
  {
    if (pg_abs_s32(idx) > n)
      idx = n;
    else
      idx = n + idx;
  }
  if (idx >= (int) n)
    return jsonb_copy(in);

  pushJsonbValue(&state, r, NULL);
  while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
  {
    if (r == WJB_ELEM)
    {
      if ((int) i++ == idx)
        continue;
    }
    res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
  }

  assert(res != NULL);
  return JsonbValueToJsonb(res);
}

/**
 * @brief Return a copy of the JSONB value with the indicated index removed
 */
Datum
datum_jsonb_delete_idx(Datum l, Datum r)
{
  return PointerGetDatum(jsonb_delete_idx_internal(DatumGetJsonbP(l),
    DatumGetInt32(r)));
}

/*****************************************************************************/

extern JsonbValue *setPath(JsonbIterator **it, Datum *path_elems,
  bool *path_nulls, int path_len, JsonbParseState **st, int level,
  JsonbValue *newval, int op_type);
extern void setPathObject(JsonbIterator **it, Datum *path_elems,
  bool *path_nulls, int path_len, JsonbParseState **st, int level,
  JsonbValue *newval, uint32 npairs, int op_type);
extern void setPathArray(JsonbIterator **it, Datum *path_elems,
  bool *path_nulls, int path_len, JsonbParseState **st, int level,
  JsonbValue *newval, uint32 nelems, int op_type);

void
push_null_elements(JsonbParseState **ps, int num)
{
  JsonbValue  null;

  null.type = jbvNull;

  while (num-- > 0)
    pushJsonbValue(ps, WJB_ELEM, &null);
}

/*
 * Prepare a new structure containing nested empty objects and arrays
 * corresponding to the specified path, and assign a new value at the end of
 * this path. E.g. the path [a][0][b] with the new value 1 will produce the
 * structure {a: [{b: 1}]}.
 *
 * Caller is responsible to make sure such path does not exist yet.
 */
static void
push_path(JsonbParseState **st, int level, Datum *path_elems,
      bool *path_nulls, int path_len, JsonbValue *newval)
{
  /*
   * tpath contains expected type of an empty jsonb created at each level
   * higher or equal to the current one, either jbvObject or jbvArray. Since
   * it contains only information about path slice from level to the end,
   * the access index must be normalized by level.
   */
  enum jbvType *tpath = palloc0((path_len - level) * sizeof(enum jbvType));
  JsonbValue  newkey;

  /*
   * Create first part of the chain with beginning tokens. For the current
   * level WJB_BEGIN_OBJECT/WJB_BEGIN_ARRAY was already created, so start
   * with the next one.
   */
  for (int i = level + 1; i < path_len; i++)
  {
    char     *c,
           *badp;
    int      lindex;

    if (path_nulls[i])
      break;

    /*
     * Try to convert to an integer to find out the expected type, object
     * or array.
     */
    c = text2cstring((text *) DatumGetPointer(path_elems[i]));
    errno = 0;
    lindex = strtoint(c, &badp, 10);
    if (badp == c || *badp != '\0' || errno != 0)
    {
      /* text, an object is expected */
      newkey.type = jbvString;
      newkey.val.string.val = c;
      newkey.val.string.len = strlen(c);

      (void) pushJsonbValue(st, WJB_BEGIN_OBJECT, NULL);
      (void) pushJsonbValue(st, WJB_KEY, &newkey);

      tpath[i - level] = jbvObject;
    }
    else
    {
      /* integer, an array is expected */
      (void) pushJsonbValue(st, WJB_BEGIN_ARRAY, NULL);

      push_null_elements(st, lindex);

      tpath[i - level] = jbvArray;
    }
  }

  /* Insert an actual value for either an object or array */
  if (tpath[(path_len - level) - 1] == jbvArray)
  {
    (void) pushJsonbValue(st, WJB_ELEM, newval);
  }
  else
    (void) pushJsonbValue(st, WJB_VALUE, newval);

  /*
   * Close everything up to the last but one level. The last one will be
   * closed outside of this function.
   */
  for (int i = path_len - 1; i > level; i--)
  {
    if (path_nulls[i])
      break;

    if (tpath[i - level] == jbvObject)
      (void) pushJsonbValue(st, WJB_END_OBJECT, NULL);
    else
      (void) pushJsonbValue(st, WJB_END_ARRAY, NULL);
  }
}

/*
 * Do most of the heavy work for jsonb_set/jsonb_insert
 */
JsonbValue *
setPath(JsonbIterator **it, Datum *path_elems, bool *path_nulls, int path_len,
    JsonbParseState **st, int level, JsonbValue *newval, int op_type)
{
  JsonbValue v;
  JsonbIteratorToken r;
  JsonbValue *res;

//  check_stack_depth();

  if (path_nulls[level])
    meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
      "path element at position %d is null", level + 1);

  r = JsonbIteratorNext(it, &v, false);

  switch (r)
  {
    case WJB_BEGIN_ARRAY:

      /*
       * If instructed complain about attempts to replace within a raw
       * scalar value. This happens even when current level is equal to
       * path_len, because the last path key should also correspond to
       * an object or an array, not raw scalar.
       */
      if ((op_type & JB_PATH_FILL_GAPS) && (level <= path_len - 1) &&
        v.val.array.rawScalar)
      {
        meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
          "Cannot replace existing key");
        return NULL;
      }

      (void) pushJsonbValue(st, r, NULL);
      setPathArray(it, path_elems, path_nulls, path_len, st, level,
             newval, v.val.array.nElems, op_type);
      r = JsonbIteratorNext(it, &v, false);
      Assert(r == WJB_END_ARRAY);
      res = pushJsonbValue(st, r, NULL);
      break;
    case WJB_BEGIN_OBJECT:
      (void) pushJsonbValue(st, r, NULL);
      setPathObject(it, path_elems, path_nulls, path_len, st, level,
              newval, v.val.object.nPairs, op_type);
      r = JsonbIteratorNext(it, &v, true);
      Assert(r == WJB_END_OBJECT);
      res = pushJsonbValue(st, r, NULL);
      break;
    case WJB_ELEM:
    case WJB_VALUE:

      /*
       * If instructed complain about attempts to replace within a
       * scalar value. This happens even when current level is equal to
       * path_len, because the last path key should also correspond to
       * an object or an array, not an element or value.
       */
      if ((op_type & JB_PATH_FILL_GAPS) && (level <= path_len - 1))
      {
        meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
          "cannot replace existing key");
        res = NULL;
      }
      else
        res = pushJsonbValue(st, r, &v);
      break;
    default:
      meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
        "unrecognized iterator result: %d", (int) r);
      res = NULL;      /* keep compiler quiet */
      break;
  }

  return res;
}

/*
 * Object walker for setPath
 */
void
setPathObject(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
        int path_len, JsonbParseState **st, int level,
        JsonbValue *newval, uint32 npairs, int op_type)
{
  text     *pathelem = NULL;
  uint32 i;
  JsonbValue  k,
        v;
  bool    done = false;

  if (level >= path_len || path_nulls[level])
    done = true;
  else
  {
    /* The path Datum could be toasted, in which case we must detoast it */
    // MEOS
    // pathelem = DatumGetTextPP(path_elems[level]);
    pathelem = DatumGetTextP(path_elems[level]);
  }

  /* empty object is a special case for create */
  if ((npairs == 0) && (op_type & JB_PATH_CREATE_OR_INSERT) &&
    (level == path_len - 1))
  {
    JsonbValue  newkey;

    newkey.type = jbvString;
    newkey.val.string.val = VARDATA_ANY(pathelem);
    newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

    (void) pushJsonbValue(st, WJB_KEY, &newkey);
    (void) pushJsonbValue(st, WJB_VALUE, newval);
  }

  for (i = 0; i < npairs; i++)
  {
    JsonbIteratorToken r = JsonbIteratorNext(it, &k, true);

    Assert(r == WJB_KEY);

    if (!done &&
      k.val.string.len == (int) VARSIZE_ANY_EXHDR(pathelem) &&
      memcmp(k.val.string.val, VARDATA_ANY(pathelem),
           k.val.string.len) == 0)
    {
      done = true;

      if (level == path_len - 1)
      {
        /*
         * called from jsonb_insert(), it forbids redefining an
         * existing value
         */
        if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER))
        {
          meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
            "cannot replace existing key");
          return;
        }

        r = JsonbIteratorNext(it, &v, true);  /* skip value */
        if (!(op_type & JB_PATH_DELETE))
        {
          (void) pushJsonbValue(st, WJB_KEY, &k);
          (void) pushJsonbValue(st, WJB_VALUE, newval);
        }
      }
      else
      {
        (void) pushJsonbValue(st, r, &k);
        setPath(it, path_elems, path_nulls, path_len,
            st, level + 1, newval, op_type);
      }
    }
    else
    {
      if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done &&
        level == path_len - 1 && i == npairs - 1)
      {
        JsonbValue  newkey;

        newkey.type = jbvString;
        newkey.val.string.val = VARDATA_ANY(pathelem);
        newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

        (void) pushJsonbValue(st, WJB_KEY, &newkey);
        (void) pushJsonbValue(st, WJB_VALUE, newval);
      }

      (void) pushJsonbValue(st, r, &k);
      r = JsonbIteratorNext(it, &v, false);
      (void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
      if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
      {
        int      walking_level = 1;

        while (walking_level != 0)
        {
          r = JsonbIteratorNext(it, &v, false);

          if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
            ++walking_level;
          if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
            --walking_level;

          (void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
        }
      }
    }
  }

  /*--
   * If we got here there are only few possibilities:
   * - no target path was found, and an open object with some keys/values was
   *   pushed into the state
   * - an object is empty, only WJB_BEGIN_OBJECT is pushed
   *
   * In both cases if instructed to create the path when not present,
   * generate the whole chain of empty objects and insert the new value
   * there.
   */
  if (!done && (op_type & JB_PATH_FILL_GAPS) && (level < path_len - 1))
  {
    JsonbValue  newkey;

    newkey.type = jbvString;
    newkey.val.string.val = VARDATA_ANY(pathelem);
    newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

    (void) pushJsonbValue(st, WJB_KEY, &newkey);
    (void) push_path(st, level, path_elems, path_nulls,
             path_len, newval);

    /* Result is closed with WJB_END_OBJECT outside of this function */
  }
}

/*
 * Array walker for setPath
 */
void
setPathArray(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
       int path_len, JsonbParseState **st, int level,
       JsonbValue *newval, uint32 nelems, int op_type)
{
  JsonbValue  v;
  int      idx;
  uint32 i;
  bool    done = false;

  /* pick correct index */
  if (level < path_len && !path_nulls[level])
  {
    char     *c = text2cstring((text *) DatumGetPointer(path_elems[level]));
    char     *badp;

    errno = 0;
    idx = strtoint(c, &badp, 10);
    if (badp == c || *badp != '\0' || errno != 0)
    {
      meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
        "path element at position %d is not an integer: \"%s\"",
        level + 1, c);
      return;
    }
  }
  else
    idx = nelems;

  if (idx < 0)
  {
    // if (pg_abs_s32(idx) > nelems)
    if (abs(idx) > nelems)
    {
      /*
       * If asked to keep elements position consistent, it's not allowed
       * to prepend the array.
       */
      if (op_type & JB_PATH_CONSISTENT_POSITION)
      {
        meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
          "path element at position %d is out of range: %d", level + 1, idx);
        return;
      }
      else
        idx = PG_INT32_MIN;
    }
    else
      idx = nelems + idx;
  }

  /*
   * Filling the gaps means there are no limits on the positive index are
   * imposed, we can set any element. Otherwise limit the index by nelems.
   */
  if (!(op_type & JB_PATH_FILL_GAPS))
  {
    if (idx > 0 && idx > (int) nelems)
      idx = nelems;
  }

  /*
   * if we're creating, and idx == INT_MIN, we prepend the new value to the
   * array also if the array is empty - in which case we don't really care
   * what the idx value is
   */
  if ((idx == INT_MIN || nelems == 0) && (level == path_len - 1) &&
    (op_type & JB_PATH_CREATE_OR_INSERT))
  {
    Assert(newval != NULL);

    if (op_type & JB_PATH_FILL_GAPS && nelems == 0 && idx > 0)
      push_null_elements(st, idx);

    (void) pushJsonbValue(st, WJB_ELEM, newval);

    done = true;
  }

  /* iterate over the array elements */
  for (i = 0; i < nelems; i++)
  {
    JsonbIteratorToken r;

    if (i == (uint32) idx && level < path_len)
    {
      done = true;

      if (level == path_len - 1)
      {
        r = JsonbIteratorNext(it, &v, true);  /* skip */

        if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_CREATE))
          (void) pushJsonbValue(st, WJB_ELEM, newval);

        /*
         * We should keep current value only in case of
         * JB_PATH_INSERT_BEFORE or JB_PATH_INSERT_AFTER because
         * otherwise it should be deleted or replaced
         */
        if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_INSERT_BEFORE))
          (void) pushJsonbValue(st, r, &v);

        if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_REPLACE))
          (void) pushJsonbValue(st, WJB_ELEM, newval);
      }
      else
        (void) setPath(it, path_elems, path_nulls, path_len,
                 st, level + 1, newval, op_type);
    }
    else
    {
      r = JsonbIteratorNext(it, &v, false);

      (void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);

      if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
      {
        int      walking_level = 1;

        while (walking_level != 0)
        {
          r = JsonbIteratorNext(it, &v, false);

          if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
            ++walking_level;
          if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
            --walking_level;

          (void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
        }
      }
    }
  }

  if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done && level == path_len - 1)
  {
    /*
     * If asked to fill the gaps, idx could be bigger than nelems, so
     * prepend the new element with nulls if that's the case.
     */
    if (op_type & JB_PATH_FILL_GAPS && idx > (int) nelems)
      push_null_elements(st, idx - nelems);

    (void) pushJsonbValue(st, WJB_ELEM, newval);
    done = true;
  }

  /*--
   * If we got here there are only few possibilities:
   * - no target path was found, and an open array with some keys/values was
   *   pushed into the state
   * - an array is empty, only WJB_BEGIN_ARRAY is pushed
   *
   * In both cases if instructed to create the path when not present,
   * generate the whole chain of empty objects and insert the new value
   * there.
   */
  if (!done && (op_type & JB_PATH_FILL_GAPS) && (level < path_len - 1))
  {
    if (idx > 0)
      push_null_elements(st, idx - nelems);

    (void) push_path(st, level, path_elems, path_nulls,
             path_len, newval);

    /* Result is closed with WJB_END_OBJECT outside of this function */
  }
}

/*****************************************************************************/

/**
 * @brief Replace a JSONB value specified by a path with a new value
 * @note Derived from the PostgreSQL function @p
 * jsonb_set(jsonb, text[], jsonb, boolean)
 */
Jsonb *
jsonb_set_internal(const Jsonb *jb, Datum *path_elems, bool *path_nulls,
  int path_len, Jsonb *newjsonb, bool create)
{
  JsonbValue newval;
  JsonbToJsonbValue(newjsonb, &newval);

  if (JB_ROOT_IS_SCALAR(jb))
    meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE, "cannot set path jb scalar");

  if (JB_ROOT_COUNT(jb) == 0 && !create)
    return jsonb_copy(jb);

  if (path_len == 0)
    return jsonb_copy(jb);

  JsonbIterator *it = JsonbIteratorInit(&((Jsonb *)jb)->root);
  JsonbParseState *st = NULL;
  JsonbValue *res = setPath(&it, path_elems, path_nulls, path_len, &st, 0,
    &newval, create ? JB_PATH_CREATE : JB_PATH_REPLACE);

  assert(res != NULL);
  return JsonbValueToJsonb(res);
}

/**
 * @brief Replace a JSONB value specified by a path with a new value
 * @note Derived from the PostgreSQL function @p
 * jsonb_insert(jsonb, text[], jsonb, boolean)
 */
Jsonb *
jsonb_insert_internal(const Jsonb *jb, Datum *path_elems, bool *path_nulls,
  int path_len, Jsonb *newjsonb, bool after)
{
  JsonbValue newval;
  JsonbToJsonbValue(newjsonb, &newval);

  if (JB_ROOT_IS_SCALAR(jb))
  {
    meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
      "cannot set path in scalar");
    return NULL;
  }

  if (path_len == 0)
    return jsonb_copy(jb);

  JsonbIterator *it = JsonbIteratorInit(&((Jsonb *)jb)->root);
  JsonbParseState *st = NULL;
  JsonbValue *res = setPath(&it, path_elems, path_nulls, path_len, &st, 0, 
    &newval, after ? JB_PATH_INSERT_AFTER : JB_PATH_INSERT_BEFORE);

  assert(res != NULL);
  return JsonbValueToJsonb(res);
}

/**
 * @brief Delete the field or array element at the specified path, where path
 * elements can be either field keys or array indexes
 * @note Derived from PostgreSQL function @p jsonb_delete_path(jsonb, text[])
 */
Jsonb *
jsonb_delete_path_internal(const Jsonb *jb, Datum *path_elems,
  bool *path_nulls, int path_len)
{
  if (JB_ROOT_IS_SCALAR(jb))
  {
    meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
      "cannot delete path jb scalar");
    return NULL;
  }

  if (JB_ROOT_COUNT(jb) == 0 || path_len == 0)
    return jsonb_copy(jb);

  JsonbIterator *it = JsonbIteratorInit(&((Jsonb *) jb)->root);
  JsonbParseState *st = NULL;
  JsonbValue *res = setPath(&it, path_elems, path_nulls, path_len, &st, 0,
    NULL, JB_PATH_DELETE);

  assert(res != NULL);
  return JsonbValueToJsonb(res);
}

/*****************************************************************************/

/*
 * Return the text representation of the given JsonbValue.
 */
static text *
JsonbValueAsText(JsonbValue *v)
{
  switch (v->type)
  {
    case jbvNull:
      return NULL;

    case jbvBool:
      return v->val.boolean ?
        cstring_to_text_with_len("true", 4) :
        cstring_to_text_with_len("false", 5);

    case jbvString:
      return cstring_to_text_with_len(v->val.string.val, v->val.string.len);

    case jbvNumeric:
      {
        char *cstr = numeric_out_internal(v->val.numeric);
        return cstring2text(cstr);
      }

    case jbvBinary:
      {
        StringInfoData jtext;

        initStringInfo(&jtext);
        (void) JsonbToCString(&jtext, v->val.binary.data,
                    v->val.binary.len);

        return cstring_to_text_with_len(jtext.data, jtext.len);
      }

    default:
      meos_error(ERROR, MEOS_ERR_INVALID_ARG_VALUE,
        "unrecognized jsonb type: %d", (int) v->type);
      return NULL;
  }
}

/**
 * @brief Extracts a JSONB object field with the given key
 * @note Derived from PostgreSQL function @p jsonb_object_field(jsonb, text)
 */
Jsonb *
jsonb_object_field_internal(const Jsonb *jb, const text *key)
{
  if (!JB_ROOT_IS_OBJECT(jb))
    return NULL;

  JsonbValue vbuf;
  JsonbValue *v = getKeyJsonValueFromContainer(&((Jsonb *)jb)->root,
    VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key), &vbuf);

  if (v != NULL)
    return JsonbValueToJsonb(v);

  return NULL;
}

/**
 * @brief Extracts a JSONB object field with the given key
 */
Datum
datum_jsonb_object_field(Datum l, Datum r)
{
  return PointerGetDatum(jsonb_object_field_internal(DatumGetJsonbP(l),
    DatumGetTextP(r)));
}

/**
 * @brief Extracts a JSONB object field with the given key
 * @note Derived from PostgreSQL function @p jsonb_object_field(jsonb, text)
 */
text *
jsonb_object_field_text_internal(const Jsonb *jb, const text *key)
{
  if (!JB_ROOT_IS_OBJECT(jb))
    return NULL;

  JsonbValue vbuf;
  JsonbValue *v = getKeyJsonValueFromContainer(&((Jsonb *)jb)->root,
    VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key), &vbuf);

  if (v != NULL && v->type != jbvNull)
    return JsonbValueAsText(v);

  return NULL;
}

/**
 * @brief Extracts a JSONB object field with the given key
 */
Datum
datum_jsonb_object_field_text(Datum l, Datum r)
{
  return PointerGetDatum(jsonb_object_field_text_internal(DatumGetJsonbP(l),
    DatumGetTextP(r)));
}

/**
 * @brief Extract a JSONB object field with the given path
 */
Jsonb *
get_jsonb_path_all_internal(const Jsonb *jb, Datum *pathtext, int path_len,
  bool as_text)
{
  bool isnull;
  Datum res = jsonb_get_element((Jsonb *) jb, pathtext, path_len, &isnull,
    as_text);
  if (isnull)
    return NULL;
  else
    return DatumGetJsonbP(res);
}

/**
 * @brief Extract a JSONB object field with the given path
 */
Jsonb *
jsonb_extract_path_internal(const Jsonb *jb, Datum *pathtext, int path_len)
{
  return get_jsonb_path_all_internal(jb, pathtext, path_len, false);
}

/**
 * @brief Extract a JSONB object field with the given path in text format
 */
Jsonb *
jsonb_extract_path_text_internal(const Jsonb *jb, Datum *pathtext,
  int path_len)
{
  return get_jsonb_path_all_internal(jb, pathtext, path_len, true);
}

/*****************************************************************************/

/**
 * @ingroup meos_jsonb_base_types
 * @brief Convert a C string into a jsonb object
 * @param[in] str String, possibly with escaped quotes
 */
Jsonb *
cstring2jsonb(const char *str)
{
  VALIDATE_NOT_NULL(str, NULL);

  /* Step 1: De-escape \" to " */
  size_t len = strlen(str);
  char *clean = palloc(len + 1);
  int j = 0;

  for (size_t i = 0; i < len; i++)
  {
    if (str[i] == '\\' && str[i + 1] == '"')
    {
      clean[j++] = '"';
      i++;
    }
    else
    {
      clean[j++] = str[i];
    }
  }
  clean[j] = '\0';

  /* Step 2: Strip outer quotes if any */
  len = strlen(clean);
  if (len >= 2 && clean[0] == '"' && clean[len - 1] == '"')
  {
    char *unquoted = palloc(len - 1);
    memcpy(unquoted, clean + 1, len - 2);
    unquoted[len - 2] = '\0';
    pfree(clean);
    clean = unquoted;
  }

  /* Step 3: Parse JSONB */
  Datum d = DirectFunctionCall1(jsonb_in, CStringGetDatum(clean));
  return DatumGetJsonbP(d);
}

/**
 * @brief Return the a jsonb value from its text representation
 * @note Funcion used by the lifting infrastructure
 */
Datum
datum_text_to_jsonb(Datum txt)
{
  char *str = text2cstring(DatumGetTextP(txt));
  Jsonb *result = cstring2jsonb(str);
  pfree(str);
  return PointerGetDatum(result);
}

extern char *
JsonbToCString(StringInfo out, JsonbContainer *in, int estimated_len);

/**
 * @ingroup meos_jsonb_base_types
 * @brief Convert a jsonb object into a C string
 * @param[in] jb JSONB object
 */
char *
jsonb2cstring(const Jsonb *jb)
{
  VALIDATE_NOT_NULL(jb, NULL);
  return JsonbToCString(NULL, &((Jsonb *) jb)->root, VARSIZE(jb));
}

/**
 * @ingroup meos_jsonb_base_types
 * @brief Return the unquoted string representation of a jsonb value
 * @param[in] jb JSONB object
 * @note Derived from PostgreSQL function @p jsonb_out(PG_FUNCTION_ARGS)
 */
char *
pg_jsonb_out(const Jsonb *jb)
{
  assert(jb);
  return jsonb2cstring(jb);
}

/**
 * @brief Return the text representation of a jsonb value
 * @note Funcion used by the lifting infrastructure
 */
Datum
datum_jsonb_to_text(Datum jb)
{
  char *str = pg_jsonb_out(DatumGetJsonbP(jb));
  text *result = cstring2text(str);
  pfree(str);
  return PointerGetDatum(result);
}

/*****************************************************************************/
