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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

public class Types
{
  private static final String ARRAY_PREFIX = "ARRAY<";
  private static final String COMPLEX_PREFIX = "COMPLEX<";

  /**
   * Create a {@link TypeSignature} given the value of {@link TypeSignature#asTypeString()} and a {@link TypeFactory}
   */
  @Nullable
  public static <T extends TypeSignature<?>> T fromString(TypeFactory<T> typeFactory, @Nullable String typeString)
  {
    if (typeString == null) {
      return null;
    }
    final String upperTypeString = StringUtils.toUpperCase(typeString);
    switch (upperTypeString) {
      case "STRING":
        return typeFactory.ofString();
      case "LONG":
        return typeFactory.ofLong();
      case "FLOAT":
        return typeFactory.ofFloat();
      case "DOUBLE":
        return typeFactory.ofDouble();
      case "STRING_ARRAY":
        return typeFactory.ofArray(typeFactory.ofString());
      case "LONG_ARRAY":
        return typeFactory.ofArray(typeFactory.ofLong());
      case "DOUBLE_ARRAY":
        return typeFactory.ofArray(typeFactory.ofDouble());
      case "COMPLEX":
        return typeFactory.ofComplex(null);
      default:
        // Prefix matching is case-insensitive, consistent with the scalar handling above, but the type parameter is
        // taken from the original string: complex type names are case-sensitive registry keys which must be preserved
        // in their original casing.
        if (upperTypeString.startsWith(ARRAY_PREFIX) && upperTypeString.endsWith(">")) {
          T elementType = fromString(typeFactory, typeString.substring(ARRAY_PREFIX.length(), typeString.length() - 1));
          if (elementType != null) {
            return typeFactory.ofArray(elementType);
          }
        }
        if (upperTypeString.startsWith(COMPLEX_PREFIX) && upperTypeString.endsWith(">")) {
          return typeFactory.ofComplex(typeString.substring(COMPLEX_PREFIX.length(), typeString.length() - 1));
        }
    }
    return null;
  }

  /**
   * Returns true if {@link TypeSignature#getType()} is of the specified {@link TypeDescriptor}
   */
  public static <T extends TypeDescriptor> boolean is(@Nullable TypeSignature<T> typeSignature, T typeDescriptor)
  {
    return typeSignature != null && typeSignature.is(typeDescriptor);
  }

  /**
   * Returns true if {@link TypeSignature#getType()} is null, or of the specified {@link TypeDescriptor}
   */
  public static <T extends TypeDescriptor> boolean isNullOr(@Nullable TypeSignature<T> typeSignature, T typeDescriptor)
  {
    return typeSignature == null || typeSignature.is(typeDescriptor);
  }

  /**
   * Returns true if the {@link TypeSignature} is null, or is any one of the specified {@link TypeDescriptor}
   */
  public static <T extends TypeDescriptor> boolean isNullOrAnyOf(
      @Nullable TypeSignature<T> typeSignature,
      T... typeDescriptors
  )
  {
    return typeSignature == null || typeSignature.anyOf(typeDescriptors);
  }

  /**
   * Returns true if either supplied {@link TypeSignature#getType()} is the given {@link TypeDescriptor}
   *
   * Useful for choosing a common {@link TypeDescriptor} between two {@link TypeSignature} when one of the signatures
   * might be null.
   */
  public static <T extends TypeDescriptor> boolean either(
      @Nullable TypeSignature<T> typeSignature1,
      @Nullable TypeSignature<T> typeSignature2,
      T typeDescriptor
  )
  {
    return (typeSignature1 != null && typeSignature1.is(typeDescriptor)) ||
           (typeSignature2 != null && typeSignature2.is(typeDescriptor));
  }

  /**
   * Returns true if {@link TypeSignature} is not null and is {@link TypeSignature#isNumeric()}
   */
  public static <T extends TypeDescriptor> boolean isNumeric(@Nullable TypeSignature<T> typeSignature)
  {
    return typeSignature != null && typeSignature.isNumeric();
  }

  /**
   * Returns true if {@link TypeSignature} is not null and is {@link TypeSignature#isNumeric()} or has
   * {@link TypeSignature#getElementType()} that is numeric.
   */
  public static <T extends TypeDescriptor> boolean isNumericOrNumericArray(@Nullable TypeSignature<T> typeSignature)
  {
    if (typeSignature == null) {
      return false;
    }
    return typeSignature.isNumeric() || (typeSignature.isArray() && typeSignature.getElementType().isNumeric());
  }

  public static class IncompatibleTypeException extends IAE
  {
    public IncompatibleTypeException(TypeSignature<?> type, TypeSignature<?> other)
    {
      super("Cannot implicitly cast [%s] to [%s]", type, other);
    }
  }

  public static class InvalidCastException extends IAE
  {
    public InvalidCastException(TypeSignature<?> type, TypeSignature<?> other)
    {
      super("Invalid type, cannot cast [" + type + "] to [" + other + "]");
    }
  }

  public static class InvalidCastBooleanException extends IAE
  {
    public InvalidCastBooleanException(TypeSignature<?> type)
    {
      super("Invalid type, cannot coerce [" + type + "] to boolean");
    }
  }
}
