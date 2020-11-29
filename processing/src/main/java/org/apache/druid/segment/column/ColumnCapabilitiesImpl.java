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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

/**
 *
 */
public class ColumnCapabilitiesImpl implements ColumnCapabilities
{
  private static final CoercionLogic ALL_FALSE = new CoercionLogic()
  {
    @Override
    public boolean dictionaryEncoded()
    {
      return false;
    }

    @Override
    public boolean dictionaryValuesSorted()
    {
      return false;
    }

    @Override
    public boolean dictionaryValuesUnique()
    {
      return false;
    }

    @Override
    public boolean multipleValues()
    {
      return false;
    }

    @Override
    public boolean hasNulls()
    {
      return false;
    }
  };

  public static ColumnCapabilitiesImpl copyOf(@Nullable final ColumnCapabilities other)
  {
    final ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
    if (other != null) {
      capabilities.type = other.getType();
      capabilities.complexTypeName = other.getComplexTypeName();
      capabilities.dictionaryEncoded = other.isDictionaryEncoded();
      capabilities.hasInvertedIndexes = other.hasBitmapIndexes();
      capabilities.hasSpatialIndexes = other.hasSpatialIndexes();
      capabilities.hasMultipleValues = other.hasMultipleValues();
      capabilities.dictionaryValuesSorted = other.areDictionaryValuesSorted();
      capabilities.dictionaryValuesUnique = other.areDictionaryValuesUnique();
      capabilities.hasNulls = other.hasNulls();
      capabilities.filterable = other.isFilterable();
    }
    return capabilities;
  }

  /**
   * Copy a {@link ColumnCapabilities} and coerce all {@link ColumnCapabilities.Capable#UNKNOWN} to
   * {@link ColumnCapabilities.Capable#TRUE} or {@link ColumnCapabilities.Capable#FALSE} as specified by
   * {@link ColumnCapabilities.CoercionLogic}
   */
  @Nullable
  public static ColumnCapabilitiesImpl snapshot(@Nullable final ColumnCapabilities capabilities, CoercionLogic coerce)
  {
    if (capabilities == null) {
      return null;
    }
    ColumnCapabilitiesImpl copy = copyOf(capabilities);
    copy.dictionaryEncoded = copy.dictionaryEncoded.coerceUnknownToBoolean(coerce.dictionaryEncoded());
    copy.dictionaryValuesSorted = copy.dictionaryValuesSorted.coerceUnknownToBoolean(coerce.dictionaryValuesSorted());
    copy.dictionaryValuesUnique = copy.dictionaryValuesUnique.coerceUnknownToBoolean(coerce.dictionaryValuesUnique());
    copy.hasMultipleValues = copy.hasMultipleValues.coerceUnknownToBoolean(coerce.multipleValues());
    copy.hasNulls = copy.hasNulls.coerceUnknownToBoolean(coerce.hasNulls());
    return copy;
  }

  /**
   * Snapshots a pair of capabilities and then merges them
   */
  @Nullable
  public static ColumnCapabilitiesImpl merge(
      @Nullable final ColumnCapabilities capabilities,
      @Nullable final ColumnCapabilities other,
      CoercionLogic coercionLogic
  )
  {
    ColumnCapabilitiesImpl merged = snapshot(capabilities, coercionLogic);
    ColumnCapabilitiesImpl otherSnapshot = snapshot(other, coercionLogic);
    if (merged == null) {
      return otherSnapshot;
    } else if (otherSnapshot == null) {
      return merged;
    }

    if (merged.type == null) {
      merged.type = other.getType();
    }

    if (!merged.type.equals(otherSnapshot.getType())) {
      throw new ISE("Cannot merge columns of type[%s] and [%s]", merged.type, otherSnapshot.getType());
    }

    if (merged.type == ValueType.COMPLEX && merged.complexTypeName == null) {
      merged.complexTypeName = other.getComplexTypeName();
    }

    if (merged.type == ValueType.COMPLEX && merged.complexTypeName != null && !merged.complexTypeName.equals(other.getComplexTypeName())) {
      throw new ISE("Cannot merge columns of typeName[%s] and [%s]", merged.complexTypeName, other.getComplexTypeName());
    }

    merged.dictionaryEncoded = merged.dictionaryEncoded.or(otherSnapshot.isDictionaryEncoded());
    merged.hasMultipleValues = merged.hasMultipleValues.or(otherSnapshot.hasMultipleValues());
    merged.dictionaryValuesSorted = merged.dictionaryValuesSorted.and(otherSnapshot.areDictionaryValuesSorted());
    merged.dictionaryValuesUnique = merged.dictionaryValuesUnique.and(otherSnapshot.areDictionaryValuesUnique());
    merged.hasNulls = merged.hasNulls.or(other.hasNulls());
    merged.hasInvertedIndexes |= otherSnapshot.hasBitmapIndexes();
    merged.hasSpatialIndexes |= otherSnapshot.hasSpatialIndexes();
    merged.filterable &= otherSnapshot.isFilterable();

    return merged;
  }

  /**
   * Creates a {@link ColumnCapabilitiesImpl} where all {@link ColumnCapabilities.Capable} that default to unknown
   * instead are coerced to true or false
   */
  public static ColumnCapabilitiesImpl createDefault()
  {
    return ColumnCapabilitiesImpl.snapshot(new ColumnCapabilitiesImpl(), ALL_FALSE);
  }

  /**
   * Create a no frills, simple column with {@link ValueType} set and everything else false
   */
  public static ColumnCapabilitiesImpl createSimpleNumericColumnCapabilities(ValueType valueType)
  {
    ColumnCapabilitiesImpl builder = new ColumnCapabilitiesImpl().setType(valueType)
                                                                 .setHasMultipleValues(false)
                                                                 .setHasBitmapIndexes(false)
                                                                 .setDictionaryEncoded(false)
                                                                 .setDictionaryValuesSorted(false)
                                                                 .setDictionaryValuesUnique(false)
                                                                 .setHasSpatialIndexes(false);
    if (NullHandling.replaceWithDefault()) {
      builder.setHasNulls(false);
    }
    return builder;
  }

  /**
   * Similar to {@link #createSimpleNumericColumnCapabilities} except {@link #hasMultipleValues} is explicitly true
   * and {@link #hasNulls} is not set
   */
  public static ColumnCapabilitiesImpl createSimpleArrayColumnCapabilities(ValueType valueType)
  {
    ColumnCapabilitiesImpl builder = new ColumnCapabilitiesImpl().setType(valueType)
                                                                 .setHasMultipleValues(true)
                                                                 .setHasBitmapIndexes(false)
                                                                 .setDictionaryEncoded(false)
                                                                 .setDictionaryValuesSorted(false)
                                                                 .setDictionaryValuesUnique(false)
                                                                 .setHasSpatialIndexes(false);
    return builder;
  }

  @Nullable
  private ValueType type = null;

  @Nullable
  private String complexTypeName = null;

  private boolean hasInvertedIndexes = false;
  private boolean hasSpatialIndexes = false;
  private Capable dictionaryEncoded = Capable.UNKNOWN;
  private Capable hasMultipleValues = Capable.UNKNOWN;

  // These capabilities are computed at query time and not persisted in the segment files.
  @JsonIgnore
  private Capable dictionaryValuesSorted = Capable.UNKNOWN;
  @JsonIgnore
  private Capable dictionaryValuesUnique = Capable.UNKNOWN;
  @JsonIgnore
  private boolean filterable;
  @JsonIgnore
  private Capable hasNulls = Capable.UNKNOWN;

  @Override
  @JsonProperty
  public ValueType getType()
  {
    return type;
  }

  @Override
  @JsonProperty
  public String getComplexTypeName()
  {
    return complexTypeName;
  }

  public ColumnCapabilitiesImpl setType(ValueType type)
  {
    this.type = Preconditions.checkNotNull(type, "'type' must be nonnull");
    return this;
  }

  public ColumnCapabilitiesImpl setComplexTypeName(String typeName)
  {
    this.complexTypeName = typeName;
    return this;
  }

  @Override
  @JsonProperty("dictionaryEncoded")
  public Capable isDictionaryEncoded()
  {
    return dictionaryEncoded;
  }

  @JsonSetter("dictionaryEncoded")
  public ColumnCapabilitiesImpl setDictionaryEncoded(boolean dictionaryEncoded)
  {
    this.dictionaryEncoded = Capable.of(dictionaryEncoded);
    return this;
  }

  @Override
  public Capable areDictionaryValuesSorted()
  {
    return dictionaryValuesSorted;
  }

  public ColumnCapabilitiesImpl setDictionaryValuesSorted(boolean dictionaryValuesSorted)
  {
    this.dictionaryValuesSorted = Capable.of(dictionaryValuesSorted);
    return this;
  }

  @Override
  public Capable areDictionaryValuesUnique()
  {
    return dictionaryValuesUnique;
  }

  public ColumnCapabilitiesImpl setDictionaryValuesUnique(boolean dictionaryValuesUnique)
  {
    this.dictionaryValuesUnique = Capable.of(dictionaryValuesUnique);
    return this;
  }

  @Override
  @JsonProperty("hasBitmapIndexes")
  public boolean hasBitmapIndexes()
  {
    return hasInvertedIndexes;
  }

  public ColumnCapabilitiesImpl setHasBitmapIndexes(boolean hasInvertedIndexes)
  {
    this.hasInvertedIndexes = hasInvertedIndexes;
    return this;
  }

  @Override
  @JsonProperty("hasSpatialIndexes")
  public boolean hasSpatialIndexes()
  {
    return hasSpatialIndexes;
  }

  public ColumnCapabilitiesImpl setHasSpatialIndexes(boolean hasSpatialIndexes)
  {
    this.hasSpatialIndexes = hasSpatialIndexes;
    return this;
  }

  @Override
  @JsonProperty("hasMultipleValues")
  public Capable hasMultipleValues()
  {
    return hasMultipleValues;
  }

  public ColumnCapabilitiesImpl setHasMultipleValues(boolean hasMultipleValues)
  {
    this.hasMultipleValues = Capable.of(hasMultipleValues);
    return this;
  }

  @Override
  public Capable hasNulls()
  {
    return hasNulls;
  }

  public ColumnCapabilitiesImpl setHasNulls(boolean hasNulls)
  {
    this.hasNulls = Capable.of(hasNulls);
    return this;
  }

  public ColumnCapabilitiesImpl setHasNulls(Capable hasNulls)
  {
    this.hasNulls = hasNulls;
    return this;
  }

  @Override
  public boolean isFilterable()
  {
    return type == ValueType.STRING ||
           type == ValueType.LONG ||
           type == ValueType.FLOAT ||
           type == ValueType.DOUBLE ||
           filterable;
  }

  public ColumnCapabilitiesImpl setFilterable(boolean filterable)
  {
    this.filterable = filterable;
    return this;
  }
}
