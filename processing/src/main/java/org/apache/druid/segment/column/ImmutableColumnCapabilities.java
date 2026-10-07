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

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Immutable {@link ColumnCapabilities}. Instances obtained from {@link #internedOf} are interned.
 */
public final class ImmutableColumnCapabilities implements ColumnCapabilities
{
  private static final Interner<ImmutableColumnCapabilities> INTERNER = Interners.newWeakInterner();

  private final ValueType type;
  @Nullable
  private final String complexTypeName;
  @Nullable
  private final TypeSignature<ValueType> elementType;
  private final Capable dictionaryEncoded;
  private final Capable dictionaryValuesSorted;
  private final Capable dictionaryValuesUnique;
  private final Capable hasMultipleValues;
  private final boolean hasBitmapIndexes;
  private final boolean hasSpatialIndexes;
  private final Capable hasNulls;

  private ImmutableColumnCapabilities(ColumnCapabilities capabilities)
  {
    this.type = capabilities.getType();
    this.complexTypeName = capabilities.getComplexTypeName();
    this.elementType = capabilities.getElementType();
    this.dictionaryEncoded = capabilities.isDictionaryEncoded();
    this.dictionaryValuesSorted = capabilities.areDictionaryValuesSorted();
    this.dictionaryValuesUnique = capabilities.areDictionaryValuesUnique();
    this.hasMultipleValues = capabilities.hasMultipleValues();
    this.hasBitmapIndexes = capabilities.hasBitmapIndexes();
    this.hasSpatialIndexes = capabilities.hasSpatialIndexes();
    this.hasNulls = capabilities.hasNulls();
  }

  /**
   * Returns an interned, immutable copy of the given capabilities.
   */
  public static ImmutableColumnCapabilities internedOf(ColumnCapabilities capabilities)
  {
    if (capabilities instanceof ImmutableColumnCapabilities) {
      return INTERNER.intern((ImmutableColumnCapabilities) capabilities);
    }
    return INTERNER.intern(new ImmutableColumnCapabilities(capabilities));
  }

  @Override
  public ValueType getType()
  {
    return type;
  }

  @Nullable
  @Override
  public String getComplexTypeName()
  {
    return complexTypeName;
  }

  @Nullable
  @Override
  public TypeSignature<ValueType> getElementType()
  {
    return elementType;
  }

  @Override
  public Capable isDictionaryEncoded()
  {
    return dictionaryEncoded;
  }

  @Override
  public Capable areDictionaryValuesSorted()
  {
    return dictionaryValuesSorted;
  }

  @Override
  public Capable areDictionaryValuesUnique()
  {
    return dictionaryValuesUnique;
  }

  @Override
  public Capable hasMultipleValues()
  {
    return hasMultipleValues;
  }

  @Override
  public boolean hasBitmapIndexes()
  {
    return hasBitmapIndexes;
  }

  @Override
  public boolean hasSpatialIndexes()
  {
    return hasSpatialIndexes;
  }

  @Override
  public Capable hasNulls()
  {
    return hasNulls;
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
    final ImmutableColumnCapabilities that = (ImmutableColumnCapabilities) o;
    return hasBitmapIndexes == that.hasBitmapIndexes
           && hasSpatialIndexes == that.hasSpatialIndexes
           && type == that.type
           && Objects.equals(complexTypeName, that.complexTypeName)
           && Objects.equals(elementType, that.elementType)
           && dictionaryEncoded == that.dictionaryEncoded
           && dictionaryValuesSorted == that.dictionaryValuesSorted
           && dictionaryValuesUnique == that.dictionaryValuesUnique
           && hasMultipleValues == that.hasMultipleValues
           && hasNulls == that.hasNulls;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        type,
        complexTypeName,
        elementType,
        dictionaryEncoded,
        dictionaryValuesSorted,
        dictionaryValuesUnique,
        hasMultipleValues,
        hasBitmapIndexes,
        hasSpatialIndexes,
        hasNulls
    );
  }

  @Override
  public String toString()
  {
    return "ImmutableColumnCapabilities{" +
           "type=" + asTypeString() +
           ", dictionaryEncoded=" + dictionaryEncoded +
           ", dictionaryValuesSorted=" + dictionaryValuesSorted +
           ", dictionaryValuesUnique=" + dictionaryValuesUnique +
           ", hasMultipleValues=" + hasMultipleValues +
           ", hasBitmapIndexes=" + hasBitmapIndexes +
           ", hasSpatialIndexes=" + hasSpatialIndexes +
           ", hasNulls=" + hasNulls +
           '}';
  }
}
