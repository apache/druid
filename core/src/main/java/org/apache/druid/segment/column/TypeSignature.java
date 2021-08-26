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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Objects;

public interface TypeSignature<Type extends TypeDescriptor>
{
  /**
   * {@link TypeDescriptor} enumeration used to handle different classes of types
   *
   * @see ValueType
   * @see org.apache.druid.math.expr.ExprType
   */
  Type getType();

  /**
   * Type name of 'complex' types ({@link ValueType#COMPLEX}, {@link org.apache.druid.math.expr.ExprType#COMPLEX}),
   * which are 'registered' by their name, acting as a key to get the correct set of serialization, deserialization,
   * and other type specific handling facilties.
   *
   * For other types, this value will be null.
   */
  @Nullable
  String getComplexTypeName();

  /**
   * {@link TypeSignature} for the elements contained in an array type ({@link ValueType#ARRAY},
   * {@link org.apache.druid.math.expr.ExprType#ARRAY}).
   *
   * For non-array types, this value will be null.
   */
  @Nullable
  TypeSignature<Type> getElementType();

  /**
   * Check if the value of {@link #getType()} is equal to the candidate {@link TypeDescriptor}.
   */
  default boolean is(Type candidate)
  {
    return Objects.equals(getType(), candidate);
  }

  /**
   * Check if the value of {@link #getType()} matches any of the {@link TypeDescriptor} specified.
   */
  default boolean anyOf(Type... types)
  {
    for (Type candidate : types) {
      if (Objects.equals(getType(), candidate)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if the type is numeric ({@link TypeDescriptor#isNumeric()})
   */
  @JsonIgnore
  default boolean isNumeric()
  {
    return getType().isNumeric();
  }

  /**
   * Check if the type is a primitive ({@link TypeDescriptor#isPrimitive()}, e.g. not an array, not a complex type)
   */
  @JsonIgnore
  default boolean isPrimitive()
  {
    return getType().isPrimitive();
  }

  /**
   * Check if the type is an array ({@link TypeDescriptor#isArray()})
   */
  @JsonIgnore
  default boolean isArray()
  {
    return getType().isArray();
  }

  @JsonIgnore
  default String asTypeString()
  {
    if (isArray()) {
      return StringUtils.format("ARRAY<%s>", getElementType());
    }
    final String complexTypeName = getComplexTypeName();
    if (!isPrimitive()) {
      return complexTypeName == null ? "COMPLEX" : StringUtils.format("COMPLEX<%s>", complexTypeName);
    }
    return getType().toString();
  }
}
