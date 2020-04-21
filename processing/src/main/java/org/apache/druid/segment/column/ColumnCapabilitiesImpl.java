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

import javax.annotation.Nullable;

/**
 *
 */
public class ColumnCapabilitiesImpl implements ColumnCapabilities
{
  public static ColumnCapabilitiesImpl copyOf(final ColumnCapabilities other)
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

  public static ColumnCapabilitiesImpl complete(final ColumnCapabilities capabilities, boolean convertUnknownToTrue)
  {
    ColumnCapabilitiesImpl copy = copyOf(capabilities);
    copy.hasMultipleValues = copy.hasMultipleValues.complete(convertUnknownToTrue);
    copy.dictionaryValuesSorted = copy.dictionaryValuesSorted.complete(convertUnknownToTrue);
    copy.dictionaryValuesUnique = copy.dictionaryValuesUnique.complete(convertUnknownToTrue);
    return copy;
  }


  /**
   * Create a no frills, simple column with {@link ValueType} set and everything else false
   */
  public static ColumnCapabilitiesImpl createSimpleNumericColumn(ValueType valueType)
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
}
