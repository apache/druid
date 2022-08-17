/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

/**
 * This enumeration defines the high level classification of the Druid type system, used by {@link ColumnType} to
 * indicate the type of data stored in columns and produced by expressions and aggregations, used to allow query
 * processing engine algorithms to compute results, used to compute query result row signatures, and all other type
 * needs.
 *
 * Currently, only the primitive types ({@link #isPrimitive()} is true) and {@link #COMPLEX} can be stored in columns
 * and are also the only types handled directly by the query engines. Array types can currently be produced by
 * expressions and by some post-aggregators, but do not currently have special engine handling, and should be used by
 * implementors sparingly until full engine support is in place. Aggregators should never specify array types as their
 * output type until the engines fully support these types.
 *
 * @see ColumnType
 * @see TypeSignature
 */
public enum ValueType implements TypeDescriptor
{
  /**
   * 64-bit double precision floating point number primitive type. This type may be used as a grouping key, or as an
   * input to any aggregators which support primitive numerical operations like sums, minimums, maximums, etc, as well
   * as an input to expression virtual columns.
   */
  DOUBLE,
  /**
   * 32-bit single precision floating point number primitive type. This type may be used as a grouping key, or as an
   * input to any aggregators which support primitive numerical operations like sums, minimums, maximums, etc, as well
   * as an input to expression virtual columns.
   */
  FLOAT,
  /**
   * 64-bit integer number primitve type. This type may be used as a grouping key, or as an
   * input to any aggregators which support primitive numerical operations like sums, minimums, maximums, etc, as well
   * as an input to expression virtual columns.
   */
  LONG,
  /**
   * String object type. This type may be used as a grouping key, an input to certain types of complex sketch
   * aggregators, and as an input to expression virtual columns. String types might potentially be 'multi-valued' when
   * stored in segments, and contextually at various layers of query processing, but this information is not available
   * at this level.
   *
   * Strings are typically represented as {@link String}, but multi-value strings might also appear as a
   * {@link java.util.List<String>}.
   */
  STRING,
  /**
   * Placeholder for arbitrary 'complex' types, which have a corresponding serializer/deserializer implementation. Note
   * that knowing a type is complex alone isn't enough information to work with it directly, and additional information
   * in the form of a type name which must be registered in the complex type registry. This type is not currently
   * supported as a grouping key for aggregations, and might only be supported by the specific aggregators crafted to
   * handle this complex type. Filtering on this type with standard filters will most likely have limited support, and
   * may match as a "null" value.
   *
   * These types are represented by the individual Java type associated with the complex type name as defined in the
   * type registry.
   */
  COMPLEX,

  /**
   * Placeholder for arbitrary arrays of other {@link ValueType}. This type has limited support as a grouping
   * key for aggregations, cannot be used as an input for numerical primitive aggregations such as sums, and may have
   * limited support as an input among complex type sketch aggregators.
   *
   * There are currently no native ARRAY typed columns, but they may be produced by expression virtual columns,
   * aggregators, and post-aggregators.
   *
   * Arrays are typically represented as Object[], but may also be Java primitive arrays such as long[], double[], or
   * float[]. There are additionally some exceptions in the grouping engine and SQL result layer, which translate
   * arrays to lists. Long term, this will settle on Object[] and primitive arrays, with list representation being
   * dropped internally and only used at the surface layer (such as JDBC) where necessary.
   */
  ARRAY;


  /**
   * Type is a numeric type, not including numeric array types
   */
  @Override
  public boolean isNumeric()
  {
    return isNumeric(this);
  }

  /**
   * Type is an array type
   */
  @Override
  public boolean isArray()
  {
    return isArray(this);
  }

  /**
   * Type is a 'primitive' type, which includes the {@link #isNumeric} types and {@link #STRING}, but not
   * {@link #COMPLEX} or {@link #ARRAY} types.
   *
   * Primitive types support being used for grouping to compute aggregates in both group by and top-n query engines,
   * while non-primitive types currently do not.
   */
  @Override
  public boolean isPrimitive()
  {
    return this.equals(ValueType.STRING) || isNumeric(this);
  }

  @Nullable
  @JsonCreator
  public static ValueType fromString(@Nullable String name)
  {
    if (name == null) {
      return null;
    }
    return valueOf(StringUtils.toUpperCase(name));
  }

  public static boolean isNumeric(ValueType type)
  {
    return type == ValueType.LONG || type == ValueType.FLOAT || type == ValueType.DOUBLE;
  }

  public static boolean isArray(ValueType type)
  {
    return type == ValueType.ARRAY;
  }
}
