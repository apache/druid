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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

public class Types
{
  private static final String ARRAY_PREFIX = "ARRAY<";
  private static final String COMPLEX_PREFIX = "COMPLEX<";

  @Nullable
  public static <T extends TypeSignature<?>> T fromString(TypeFactory<T> typeFactory, @Nullable String typeString)
  {
    if (typeString == null) {
      return null;
    }
    switch (StringUtils.toUpperCase(typeString)) {
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
        // we do not convert to uppercase here, because complex type name must be preserved in original casing
        // array could be converted, but are not for no particular reason other than less spooky magic
        if (typeString.startsWith(ARRAY_PREFIX)) {
          T elementType = fromString(typeFactory, typeString.substring(ARRAY_PREFIX.length(), typeString.length() - 1));
          Preconditions.checkNotNull(elementType, "Array element type must not be null");
          return typeFactory.ofArray(elementType);
        }
        if (typeString.startsWith(COMPLEX_PREFIX)) {
          return typeFactory.ofComplex(typeString.substring(COMPLEX_PREFIX.length(), typeString.length() - 1));
        }
    }
    return null;
  }

  public static <T extends TypeDescriptor> boolean is(@Nullable TypeSignature<T> typeSignature, T typeDescriptor)
  {
    return typeSignature != null && typeSignature.is(typeDescriptor);
  }

  public static <T extends TypeDescriptor> boolean isNullOr(@Nullable TypeSignature<T> typeSignature, T typeDescriptor)
  {
    return typeSignature == null || typeSignature.is(typeDescriptor);
  }

  public static <T extends TypeDescriptor> boolean anyOf(@Nullable TypeSignature<T> typeSignature, T... typeDescriptors)
  {
    return typeSignature != null && typeSignature.anyOf(typeDescriptors);
  }

  public static <T extends TypeDescriptor> boolean isNullOrAnyOf(@Nullable TypeSignature<T> typeSignature, T... typeDescriptors)
  {
    return typeSignature == null || typeSignature.anyOf(typeDescriptors);
  }

  public static <T extends TypeDescriptor> boolean either(@Nullable TypeSignature<T> typeSignature1, @Nullable TypeSignature<T> typeSignature2, T typeDescriptor)
  {
    return (typeSignature1 != null && typeSignature1.is(typeDescriptor)) || (typeSignature2 != null && typeSignature2.is(typeDescriptor));
  }
}
