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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import javax.annotation.Nullable;
import java.util.Objects;

public abstract class BaseTypeSignature<Type extends TypeDescriptor> implements TypeSignature<Type>
{
  protected final Type type;

  @Nullable
  protected final String complexTypeName;

  @Nullable
  protected final TypeSignature<Type> elementType;

  private final Supplier<TypeStrategy> typeStrategy;
  private final Supplier<NullableTypeStrategy> nullableTypeStrategy;

  public BaseTypeSignature(
      TypeFactory typeFactory,
      Type type,
      @Nullable String complexTypeName,
      @Nullable TypeSignature<Type> elementType
  )
  {
    this.type = type;
    this.complexTypeName = complexTypeName;
    this.elementType = elementType;
    this.typeStrategy = Suppliers.memoize(() -> typeFactory.getTypeStrategy(this));
    this.nullableTypeStrategy = Suppliers.memoize(() -> new NullableTypeStrategy<>(typeStrategy.get()));
  }

  @Override
  @JsonProperty("type")
  public Type getType()
  {
    return type;
  }

  @Override
  @Nullable
  @JsonProperty("complexTypeName")
  public String getComplexTypeName()
  {
    return complexTypeName;
  }

  @Override
  @Nullable
  @JsonProperty("elementType")
  public TypeSignature<Type> getElementType()
  {
    return elementType;
  }

  @Override
  public <T> TypeStrategy<T> getStrategy()
  {
    return typeStrategy.get();
  }

  @Override
  public <T> NullableTypeStrategy<T> getNullableStrategy()
  {
    return nullableTypeStrategy.get();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseTypeSignature<?> that = (BaseTypeSignature<?>) o;
    return type.equals(that.type)
           && Objects.equals(complexTypeName, that.complexTypeName)
           && Objects.equals(elementType, that.elementType);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, complexTypeName, elementType);
  }

  @Override
  public String toString()
  {
    // this is also used for JSON serialization by the sub-classes, but class is decorated with JSON annotations
    // to leave the option open someday of switching to standard serialization
    return asTypeString();
  }
}
