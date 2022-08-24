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
   * Strings are typically represented as {@link String}. Multi-value strings appear as {@link java.util.List<String>}
   * when necessary to represent multiple values, and can vary between string and list from one row to the next.
   */
  STRING,
  /**
   * Placeholder for arbitrary 'complex' types, which have a corresponding serializer/deserializer implementation. Note
   * that knowing a type is complex alone isn't enough information to work with it directly, and additional information
   * in the form of a type name which must be registered in the complex type registry. Complex types are not currently
   * supported as a grouping key for aggregations. Complex types can be used as inputs to aggregators, in cases where
   * the specific aggregator supports the specific complex type. Filtering on these types with standard filters is not
   * well supported, and will be treated as null values.
   *
   * These types are represented by the individual Java type associated with the complex type name as defined in the
   * type registry.
   */
  COMPLEX,

  /**
   * Placeholder for arbitrary arrays of other {@link ValueType}. This type has limited support as a grouping
   * key for aggregations, ARRAY of STRING, LONG, DOUBLE, and FLOAT are supported, but ARRAY types in general are not.
   * ARRAY types cannot be used as an input for numerical primitive aggregations such as sums, and have limited support
   * as an input among complex type sketch aggregators.
   *
   * There are currently no native ARRAY typed columns, but they may be produced by expression virtual columns,
   * aggregators, and post-aggregators.
   *
   * Arrays are represented as Object[], long[], double[], or float[]. The preferred type is Object[], since the
   * expression system is the main consumer of arrays, and the expression system uses Object[] internally. Some code
   * represents arrays in other ways; in particular the groupBy engine and SQL result layer. Over time we expect these
   * usages to migrate to Object[], long[], double[], and float[].
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
