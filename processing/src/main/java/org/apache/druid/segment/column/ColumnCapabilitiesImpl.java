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
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

/**
 *
 */
public class ColumnCapabilitiesImpl implements ColumnCapabilities
{
  public static ColumnCapabilitiesImpl copyOf(@Nullable final ColumnCapabilities other)
  {
    final ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
    if (other != null) {
      capabilities.type = other.getType();
      capabilities.dictionaryEncoded = other.isDictionaryEncoded();
      capabilities.runLengthEncoded = other.isRunLengthEncoded();
      capabilities.hasInvertedIndexes = other.hasBitmapIndexes();
      capabilities.hasSpatialIndexes = other.hasSpatialIndexes();
      capabilities.hasMultipleValues = other.hasMultipleValues();
      capabilities.dictionaryValuesSorted = other.areDictionaryValuesSorted();
      capabilities.dictionaryValuesUnique = other.areDictionaryValuesUnique();
      capabilities.filterable = other.isFilterable();
    }
    return capabilities;
  }

  /**
   * Used at indexing time to finalize all {@link Capable#UNKNOWN} values to
   * {@link Capable#FALSE}, in order to present a snapshot of the state of the this column
   */
  @Nullable
  public static ColumnCapabilitiesImpl snapshot(@Nullable final ColumnCapabilities capabilities)
  {
    return snapshot(capabilities, false);
  }

  /**
   * Used at indexing time to finalize all {@link Capable#UNKNOWN} values to
   * {@link Capable#FALSE} or {@link Capable#TRUE}, in order to present a snapshot of the state of the this column
   */
  @Nullable
  public static ColumnCapabilitiesImpl snapshot(@Nullable final ColumnCapabilities capabilities, boolean unknownIsTrue)
  {
    if (capabilities == null) {
      return null;
    }
    ColumnCapabilitiesImpl copy = copyOf(capabilities);
    copy.hasMultipleValues = copy.hasMultipleValues.coerceUnknownToBoolean(unknownIsTrue);
    copy.dictionaryValuesSorted = copy.dictionaryValuesSorted.coerceUnknownToBoolean(unknownIsTrue);
    copy.dictionaryValuesUnique = copy.dictionaryValuesUnique.coerceUnknownToBoolean(unknownIsTrue);
    return copy;
  }


  /**
   * Create a no frills, simple column with {@link ValueType} set and everything else false
   */
  public static ColumnCapabilitiesImpl createSimpleNumericColumnCapabilities(ValueType valueType)
  {
    return new ColumnCapabilitiesImpl().setType(valueType)
                                       .setHasMultipleValues(false)
                                       .setHasBitmapIndexes(false)
                                       .setDictionaryEncoded(false)
                                       .setDictionaryValuesSorted(false)
                                       .setDictionaryValuesUnique(false)
                                       .setHasSpatialIndexes(false);
  }

  @Nullable
  private ValueType type = null;

  private boolean dictionaryEncoded = false;
  private boolean runLengthEncoded = false;
  private boolean hasInvertedIndexes = false;
  private boolean hasSpatialIndexes = false;
  private Capable hasMultipleValues = Capable.UNKNOWN;

  // These capabilities are computed at query time and not persisted in the segment files.
  @JsonIgnore
  private Capable dictionaryValuesSorted = Capable.UNKNOWN;
  @JsonIgnore
  private Capable dictionaryValuesUnique = Capable.UNKNOWN;
  @JsonIgnore
  private boolean filterable;

  @Override
  @JsonProperty
  public ValueType getType()
  {
    return type;
  }

  public ColumnCapabilitiesImpl setType(ValueType type)
  {
    this.type = Preconditions.checkNotNull(type, "'type' must be nonnull");
    return this;
  }

  @Override
  @JsonProperty
  public boolean isDictionaryEncoded()
  {
    return dictionaryEncoded;
  }

  public ColumnCapabilitiesImpl setDictionaryEncoded(boolean dictionaryEncoded)
  {
    this.dictionaryEncoded = dictionaryEncoded;
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
  @JsonProperty
  public boolean isRunLengthEncoded()
  {
    return runLengthEncoded;
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

  public ColumnCapabilities merge(@Nullable ColumnCapabilities other)
  {
    if (other == null) {
      return this;
    }

    if (type == null) {
      type = other.getType();
    }

    if (!type.equals(other.getType())) {
      throw new ISE("Cannot merge columns of type[%s] and [%s]", type, other.getType());
    }

    this.dictionaryEncoded |= other.isDictionaryEncoded();
    this.runLengthEncoded |= other.isRunLengthEncoded();
    this.hasInvertedIndexes |= other.hasBitmapIndexes();
    this.hasSpatialIndexes |= other.hasSpatialIndexes();
    this.filterable &= other.isFilterable();
    this.hasMultipleValues = this.hasMultipleValues.or(other.hasMultipleValues());
    this.dictionaryValuesSorted = this.dictionaryValuesSorted.and(other.areDictionaryValuesSorted());
    this.dictionaryValuesUnique = this.dictionaryValuesUnique.and(other.areDictionaryValuesUnique());

    return this;
  }
}
