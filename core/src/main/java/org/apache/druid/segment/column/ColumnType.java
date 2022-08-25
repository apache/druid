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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import javax.annotation.Nullable;

/**
 * Native Druid types.
 *
 * @see TypeSignature
 */
@JsonSerialize(using = ToStringSerializer.class)
public class ColumnType extends BaseTypeSignature<ValueType>
{
  /**
   * Druid string type. Values will be represented as {@link String} or {@link java.util.List<String>} in the case
   * of multi-value string columns. {@link ColumnType} has insufficient information to distinguish between single
   * and multi-value strings, this requires a specific {@link TypeSignature} implementation which is supplied by
   * segments, 'ColumnCapabilities', which is available at a higher layer and provides the method 'hasMultipleValues'.
   *
   * @see ValueType#STRING
   */
  public static final ColumnType STRING = new ColumnType(ValueType.STRING, null, null);

  /**
   * Druid 64-bit integer number primitve type. Values will be represented as Java long or {@link Long}.
   *
   * @see ValueType#LONG
   */
  public static final ColumnType LONG = new ColumnType(ValueType.LONG, null, null);
  /**
   * Druid 64-bit double precision floating point number primitive type. Values will be represented as Java double or
   * {@link Double}.
   *
   * @see ValueType#DOUBLE
   */
  public static final ColumnType DOUBLE = new ColumnType(ValueType.DOUBLE, null, null);
  /**
   * Druid 32-bit single precision floating point number primitive type. Values will be represented as Java float or
   * {@link Float}.
   *
   * @see ValueType#FLOAT
   */
  public static final ColumnType FLOAT = new ColumnType(ValueType.FLOAT, null, null);
  // currently, arrays only come from expressions or aggregators
  /**
   * An array of Strings. Values will be represented as Object[]
   * @see ValueType#ARRAY
   * @see ValueType#STRING
   */
  public static final ColumnType STRING_ARRAY = new ColumnType(ValueType.ARRAY, null, STRING);
  /**
   * An array of Longs. Values will be represented as Object[] or long[]
   * @see ValueType#ARRAY
   * @see ValueType#LONG
   */
  public static final ColumnType LONG_ARRAY = new ColumnType(ValueType.ARRAY, null, LONG);
  /**
   * An array of Doubles. Values will be represented as Object[] or double[].
   * @see ValueType#ARRAY
   * @see ValueType#DOUBLE
   */
  public static final ColumnType DOUBLE_ARRAY = new ColumnType(ValueType.ARRAY, null, DOUBLE);
  /**
   * An array of Floats. Values will be represented as Object[] or float[].
   * @see ValueType#ARRAY
   * @see ValueType#FLOAT
   */
  public static final ColumnType FLOAT_ARRAY = new ColumnType(ValueType.ARRAY, null, FLOAT);
  /**
   * Placeholder type for an "unknown" complex, which is used when the complex type name was "lost" or unavailable for
   * whatever reason, to indicate an opaque type that cannot be generically handled with normal complex type handling
   * mechanisms. Prefer to use a {@link ColumnType} with the {@link #complexTypeName} set for most complex type matters
   * if at all possible.
   *
   * @see ValueType#COMPLEX
   */
  public static final ColumnType UNKNOWN_COMPLEX = new ColumnType(ValueType.COMPLEX, null, null);

  @JsonCreator
  public ColumnType(
      @JsonProperty("type") ValueType type,
      @JsonProperty("complexTypeName") @Nullable String complexTypeName,
      @JsonProperty("elementType") @Nullable ColumnType elementType
  )
  {
    super(ColumnTypeFactory.getInstance(), type, complexTypeName, elementType);
  }

  @Nullable
  @JsonCreator
  public static ColumnType fromString(@Nullable String typeName)
  {
    return Types.fromString(ColumnTypeFactory.getInstance(), typeName);
  }

  public static ColumnType ofArray(ColumnType elementType)
  {
    return ColumnTypeFactory.getInstance().ofArray(elementType);
  }
  public static ColumnType ofComplex(@Nullable String complexTypeName)
  {
    return ColumnTypeFactory.getInstance().ofComplex(complexTypeName);
  }
}
