/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.ISE;

/**
 */
public class ColumnCapabilitiesImpl implements ColumnCapabilities
{
  private ValueType type = null;
  private boolean dictionaryEncoded = false;
  private boolean runLengthEncoded = false;
  private boolean hasInvertedIndexes = false;
  private boolean hasSpatialIndexes = false;
  private boolean hasMultipleValues = false;

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

  public ColumnCapabilitiesImpl setRunLengthEncoded(boolean runLengthEncoded)
  {
    this.runLengthEncoded = runLengthEncoded;
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
  public boolean hasMultipleValues()
  {
    return hasMultipleValues;
  }

  public ColumnCapabilitiesImpl setHasMultipleValues(boolean hasMultipleValues)
  {
    this.hasMultipleValues = hasMultipleValues;
    return this;
  }

  @Override
  public ColumnCapabilitiesImpl merge(ColumnCapabilities other)
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
    this.hasMultipleValues |= other.hasMultipleValues();

    return this;
  }
}
