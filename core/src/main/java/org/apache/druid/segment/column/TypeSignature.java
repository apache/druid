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

/**
 * This interface serves as a common foundation for Druids native type system, and provides common methods for reasoning
 * about and handling type matters. Additional type common type handling methods are provided by {@link Types} utility.
 *
 * This information is used by Druid to make decisions about how to correctly process inputs and determine output types
 * at all layers of the engine, from how to group, filter, aggregate, and transform columns up to how to best plan SQL
 * into native Druid queries.
 *
 * The native Druid type system can currently be broken down at a high level into 'primitive' types, 'array' types, and
 * 'complex' types, and this classification is defined by an enumeration which implements {@link TypeDescriptor} such
 * as {@link ValueType} for the general query engines and {@link org.apache.druid.math.expr.ExprType} for low level
 * expression processing. This is exposed via {@link #getType()}, and will be most callers first point of contact with
 * the {@link TypeSignature} when trying to decide how to handle a given input.
 *
 * Druid 'primitive' types includes strings and numeric types. Note: multi-value string columns are still considered
 * 'primitive' string types, because they do not behave as traditional arrays (unless explicitly converted to an array),
 * and are always serialized as opportunistically single valued, so whether or not any particular string column is
 * multi-valued might vary from segment to segment. The concept of multi-valued strings only exists at a very low
 * engine level and are only modeled by the ColumnCapabilities implementation of {@link TypeSignature}.
 *
 * 'array' types contain additional nested type information about the elements of an array, a reference to another
 * {@link TypeSignature} through the {@link #getElementType()} method. If {@link TypeDescriptor#isArray()} is true,
 * then {@link #getElementType()} should never return null.
 *
 * 'complex' types are Druids extensible types, which have a registry that allows these types to be defined and
 * associated with a name which is available as {@link #getComplexTypeName()}. These type names are unique, so this
 * information is used to allow handling of these 'complex' types to confirm.
 *
 * {@link TypeSignature} is currently manifested in 3 forms: {@link ColumnType} which is the high level 'native' Druid
 * type definitions using {@link ValueType}, and is used by row signatures and SQL schemas, used by callers as input
 * to various API methods, and most general purpose type handling. In 'druid-processing' there is an additional
 * type ... type, ColumnCapabilities, which is effectively a {@link ColumnType} but includes some additional
 * information for low level query processing, such as details about whether a column has indexes, dictionaries, null
 * values, is a multi-value string column, and more.
 *
 * The third is {@link org.apache.druid.math.expr.ExpressionType}, which instead of {@link ValueType} uses
 * {@link org.apache.druid.math.expr.ExprType}, and is used exclusively for handling Druid native expression evaluation.
 * {@link org.apache.druid.math.expr.ExpressionType} exists because the Druid expression system does not natively
 * handle float types, so it is essentially a mapping of {@link ColumnType} where floats are coerced to double typed
 * values. Ideally at some point Druid expressions can just handle floats directly, and these two {@link TypeSignature}
 * can be merged, which will simplify this interface to no longer need be generic, allow {@link ColumnType} to be
 * collapsed into {@link BaseTypeSignature}, and finally unify the type system.
 */
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
   * A {@link TypeStrategy} provides facilities to reading and writing values to buffers, as well as basic value
   * comparators and byte size estimation. Use {@link #getNullableStrategy()} if you need to read and write values
   * which might possibly be null and aren't handling this in a different (probably better) way.
   */
  <T> TypeStrategy<T> getStrategy();

  /**
   * A {@link NullableTypeStrategy} is a {@link TypeStrategy} which can handle reading and writing null values, at the
   * very high cost of an additional byte per value, of which a single bit is used to store
   * {@link org.apache.druid.common.config.NullHandling#IS_NULL_BYTE} or
   * {@link org.apache.druid.common.config.NullHandling#IS_NOT_NULL_BYTE} as appropriate.
   *
   * This pattern is common among buffer aggregators, which don't have access to an external memory location for more
   * efficient tracking of null values and must store this information inline with the accumulated value.
   */
  default <T> NullableTypeStrategy<T> getNullableStrategy()
  {
    return new NullableTypeStrategy<>(getStrategy());
  }


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

  /**
   * Convert a {@link TypeSignature} into a simple string. This value can be converted back into a {@link TypeSignature}
   * with {@link Types#fromString(TypeFactory, String)}.
   */
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
