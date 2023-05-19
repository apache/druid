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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;

import javax.annotation.Nullable;
import java.util.Objects;

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
   * Druid 64-bit integer number primitive type. Values will be represented as Java long or {@link Long}.
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
   *
   * @see ValueType#ARRAY
   * @see ValueType#STRING
   */
  public static final ColumnType STRING_ARRAY = ofArray(STRING);
  /**
   * An array of Longs. Values will be represented as Object[] or long[]
   *
   * @see ValueType#ARRAY
   * @see ValueType#LONG
   */
  public static final ColumnType LONG_ARRAY = ofArray(LONG);
  /**
   * An array of Doubles. Values will be represented as Object[] or double[].
   *
   * @see ValueType#ARRAY
   * @see ValueType#DOUBLE
   */
  public static final ColumnType DOUBLE_ARRAY = ofArray(DOUBLE);
  /**
   * An array of Floats. Values will be represented as Object[] or float[].
   *
   * @see ValueType#ARRAY
   * @see ValueType#FLOAT
   */
  public static final ColumnType FLOAT_ARRAY = ofArray(FLOAT);

  public static final ColumnType NESTED_DATA = ofComplex(NestedDataComplexTypeSerde.TYPE_NAME);
  /**
   * Placeholder type for an "unknown" complex, which is used when the complex type name was "lost" or unavailable for
   * whatever reason, to indicate an opaque type that cannot be generically handled with normal complex type handling
   * mechanisms. Prefer to use a {@link ColumnType} with the {@link #complexTypeName} set for most complex type matters
   * if at all possible.
   *
   * @see ValueType#COMPLEX
   */
  public static final ColumnType UNKNOWN_COMPLEX = ofComplex(null);

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

  /**
   * Finds the type that can best represent both types, or none if there is no type information.
   * If either type is null, the other type is returned. If both types are null, this method returns null as we cannot
   * determine any useful type information. If the types are {@link ValueType#COMPLEX}, they must be the same complex
   * type, else this function throws a {@link IllegalArgumentException} as the types are truly incompatible, with the
   * exception of {@link ColumnType#NESTED_DATA} which is complex and represents nested AND mixed type data so is
   * instead treated as the 'least restrictive type' if present. If both types are {@link ValueType#ARRAY}, the result
   * is an array of the result of calling this method again on {@link ColumnType#elementType}. If only one type is an
   * array, the result is an array type of calling this method on the non-array type and the array element type. After
   * arrays, if either type is {@link ValueType#STRING}, the result is {@link ValueType#STRING}. If both types are
   * numeric, then the result will be {@link ValueType#LONG} if both are longs, {@link ValueType#FLOAT} if both are
   * floats, else {@link ValueType#DOUBLE}.
   *
   * @see org.apache.druid.math.expr.ExpressionTypeConversion#function for a similar method used for expression type
   *                                                                   inference
   */
  @Nullable
  public static ColumnType leastRestrictiveType(@Nullable ColumnType type, @Nullable ColumnType other)
  {
    if (type == null) {
      return other;
    }
    if (other == null) {
      return type;
    }
    if (type.is(ValueType.COMPLEX) && other.is(ValueType.COMPLEX)) {
      if (type.getComplexTypeName() == null) {
        return other;
      }
      if (other.getComplexTypeName() == null) {
        return type;
      }
      if (!Objects.equals(type, other)) {
        throw new IAE("Cannot implicitly cast %s to %s", type, other);
      }
      return type;
    }
    // if either is nested data, use nested data, otherwise error
    if (type.is(ValueType.COMPLEX) || other.is(ValueType.COMPLEX)) {
      if (ColumnType.NESTED_DATA.equals(type) || ColumnType.NESTED_DATA.equals(other)) {
        return ColumnType.NESTED_DATA;
      }
      throw new IAE("Cannot implicitly cast %s to %s", type, other);
    }

    // arrays convert based on least restrictive element type
    if (type.isArray()) {
      if (other.equals(type.getElementType())) {
        return type;
      }
      final ColumnType commonElementType;
      if (other.isArray()) {
        commonElementType = leastRestrictiveType(
            (ColumnType) type.getElementType(),
            (ColumnType) other.getElementType()
        );
        return ColumnType.ofArray(commonElementType);
      } else {
        commonElementType = leastRestrictiveType(
            (ColumnType) type.getElementType(),
            other
        );
      }
      return ColumnType.ofArray(commonElementType);
    }
    if (other.isArray()) {
      if (type.equals(type.getElementType())) {
        return type;
      }
      final ColumnType commonElementType;

      commonElementType = leastRestrictiveType(
          type,
          (ColumnType) other.getElementType()
      );
      return ColumnType.ofArray(commonElementType);
    }
    // if either argument is a string, type becomes a string
    if (Types.is(type, ValueType.STRING) || Types.is(other, ValueType.STRING)) {
      return ColumnType.STRING;
    }

    // all numbers win over longs
    // floats vs doubles would be handled here, but we currently only support doubles...
    if (Types.is(type, ValueType.LONG) && Types.isNullOr(other, ValueType.LONG)) {
      return ColumnType.LONG;
    }
    if (Types.is(type, ValueType.FLOAT) && Types.isNullOr(other, ValueType.FLOAT)) {
      return ColumnType.FLOAT;
    }
    return ColumnType.DOUBLE;
  }
}
