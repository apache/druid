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
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

/**
 *
 */
public class ColumnCapabilitiesImpl implements ColumnCapabilities
{
  @Nullable
  private ValueType type = null;

  private boolean dictionaryEncoded = false;
  private boolean runLengthEncoded = false;
  private boolean hasInvertedIndexes = false;
  private boolean hasSpatialIndexes = false;
  private boolean hasMultipleValues = false;

  // This is a query time concept and not persisted in the segment files.
  @JsonIgnore
  private boolean filterable;


  @JsonIgnore
  private boolean complete = false;

  public static ColumnCapabilitiesImpl copyOf(final ColumnCapabilities other)
  {
    final ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
    capabilities.merge(other);
    capabilities.setFilterable(other.isFilterable());
    capabilities.setIsComplete(other.isComplete());
    return capabilities;
  }

  @Override
  @JsonProperty
  public ValueType getType()
  {
    return type;
  }

  public ColumnCapabilitiesImpl setType(ValueType type)
  {
    this.type = type;
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
  public boolean hasMultipleValues()
  {
    return hasMultipleValues;
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

  @Override
  public boolean isComplete()
  {
    return complete;
  }

  public ColumnCapabilitiesImpl setFilterable(boolean filterable)
  {
    this.filterable = filterable;
    return this;
  }

  public ColumnCapabilitiesImpl setHasMultipleValues(boolean hasMultipleValues)
  {
    this.hasMultipleValues = hasMultipleValues;
    return this;
  }

  public ColumnCapabilitiesImpl setIsComplete(boolean complete)
  {
    this.complete = complete;
    return this;
  }

  public void merge(ColumnCapabilities other)
  {
    if (other == null) {
      return;
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
    this.hasMultipleValues |= other.hasMultipleValues();
    this.complete &= other.isComplete(); // these should always be the same?
    this.filterable &= other.isFilterable();
  }
}
