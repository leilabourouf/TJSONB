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
 * @brief Sections for the documentation of the MobilityDB API: JSONB
 */

/*****************************************************************************
 * Definition of the modules of the MobilityDB API
 * These modules follow the sections of the MobilityDB documentation although
 * some subsections are merged into a single submodule
 *****************************************************************************/

/**
 * @defgroup mobilitydb_jsonb_base Functions for static JSONBs
 * @ingroup mobilitydb_jsonb
 * @brief Functions for static JSONBs
 *
 * @defgroup mobilitydb_jsonb_set Functions for JSONB sets
 * @ingroup mobilitydb_jsonb
 * @brief Functions for JSONB sets
 *
 * @defgroup mobilitydb_jsonb_inout Input and output functions
 * @ingroup mobilitydb_jsonb
 * @brief Input and output functions for temporal JSONB
 *
 * @defgroup mobilitydb_jsonb_constructor Constructor functions
 * @ingroup mobilitydb_jsonb
 * @brief Constructor functions for temporal JSONB
 *
 * @defgroup mobilitydb_jsonb_conversion Conversion functions
 * @ingroup mobilitydb_jsonb
 * @brief Conversion functions for temporal JSONB
 *
 * @defgroup mobilitydb_jsonb_accessor Accessor functions
 * @ingroup mobilitydb_jsonb
 * @brief Accessor functions for temporal JSONB
 *
 * @defgroup mobilitydb_jsonb_transf Transformation functions
 * @ingroup mobilitydb_jsonb
 * @brief Transformation functions for temporal JSONB
 *
 * @defgroup mobilitydb_jsonb_restrict Restriction functions
 * @ingroup mobilitydb_jsonb
 * @brief Restriction functions for temporal JSONB
 *
 * @defgroup mobilitydb_jsonb_dist Distance functions
 * @ingroup mobilitydb_jsonb
 * @brief Distance functions for temporal JSONB
 *
 * @defgroup mobilitydb_jsonb_comp Comparison functions
 * @ingroup mobilitydb_jsonb
 * @brief Comparison functions for temporal JSONB
 *
 *   @defgroup mobilitydb_jsonb_comp_ever Ever and always comparison functions
 *   @ingroup mobilitydb_jsonb_comp
 *   @brief Ever and always comparison functions for temporal JSONB
 *
 *   @defgroup mobilitydb_jsonb_comp_temp Temporal comparison functions
 *   @ingroup mobilitydb_jsonb_comp
 *   @brief Comparison functions for temporal JSONB
 *
 * @defgroup mobilitydb_jsonb_agg Aggregate functions
 * @ingroup mobilitydb_jsonb
 * @brief Aggregate functions for temporal JSONB
 */

/*****************************************************************************/
/**
 * @defgroup mobilitydb_jsonb_base_inout Input and output functions
 * @ingroup mobilitydb_jsonb_base
 * @brief Input and output functions for static JSONBs
 *
 * @defgroup mobilitydb_jsonb_base_constructor Constructor functions
 * @ingroup mobilitydb_jsonb_base
 * @brief Constructor functions for static JSONBs
 *
 * @defgroup mobilitydb_jsonb_base_conversion Conversion functions
 * @ingroup mobilitydb_jsonb_base
 * @brief Conversion functions for static JSONBs
 *
 * @defgroup mobilitydb_jsonb_base_accessor Accessor functions
 * @ingroup mobilitydb_jsonb_base
 * @brief Accessor functions for static JSONBs
 *
 * @defgroup mobilitydb_jsonb_base_transf Transformation functions
 * @ingroup mobilitydb_jsonb_base
 * @brief Transformation functions for static JSONBs
 *
 * @defgroup mobilitydb_jsonb_base_box Bounding box functions
 * @ingroup mobilitydb_jsonb_base
 * @brief Bounding box functions for static JSONBs
 *
 * @defgroup mobilitydb_jsonb_base_srid Spatial reference system functions
 * @ingroup mobilitydb_jsonb_base
 * @brief Spatial reference system functions for static JSONBs
 *
 * @defgroup mobilitydb_jsonb_base_comp Comparison functions
 * @ingroup mobilitydb_jsonb_base
 * @brief Comparison functions for static JSONBs
 */

/*****************************************************************************/

