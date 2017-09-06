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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilterUtils;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupReferencesManager;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class LookupDimensionSpec implements DimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x4;

  @JsonProperty
  private final String dimension;

  @JsonProperty
  private final String outputName;

  @JsonProperty
  private final LookupExtractor lookup;

  @JsonProperty
  private final boolean retainMissingValue;

  @JsonProperty
  private final String replaceMissingValueWith;

  @JsonProperty
  private final String name;

  @JsonProperty
  private final boolean optimize;

  private final LookupReferencesManager lookupReferencesManager;

  @JsonCreator
  public LookupDimensionSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("outputName") String outputName,
      @JsonProperty("lookup") LookupExtractor lookup,
      @JsonProperty("retainMissingValue") boolean retainMissingValue,
      @JsonProperty("replaceMissingValueWith") String replaceMissingValueWith,
      @JsonProperty("name") String name,
      @JacksonInject LookupReferencesManager lookupReferencesManager,
      @JsonProperty("optimize") Boolean optimize
  )
  {
    this.retainMissingValue = retainMissingValue;
    this.optimize = optimize == null ? true : optimize;
    this.replaceMissingValueWith = Strings.emptyToNull(replaceMissingValueWith);
    this.dimension = Preconditions.checkNotNull(dimension, "dimension can not be Null");
    this.outputName = Preconditions.checkNotNull(outputName, "outputName can not be Null");
    this.lookupReferencesManager = lookupReferencesManager;
    this.name = name;
    this.lookup = lookup;
    Preconditions.checkArgument(
        Strings.isNullOrEmpty(name) ^ (lookup == null),
        "name [%s] and lookup [%s] are mutually exclusive please provide either a name or a lookup", name, lookup
    );

    if (!Strings.isNullOrEmpty(name)) {
      Preconditions.checkNotNull(
          this.lookupReferencesManager,
          "The system is not configured to allow for lookups, please read about configuring a lookup manager in the docs"
      );
    }
  }

  @Override
  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public ValueType getOutputType()
  {
    // Extraction functions always output String
    return ValueType.STRING;
  }

  @JsonProperty
  @Nullable
  public LookupExtractor getLookup()
  {
    return lookup;
  }

  @JsonProperty
  @Nullable
  public String getName()
  {
    return name;
  }

  @Override
  public ExtractionFn getExtractionFn()
  {
    final LookupExtractor lookupExtractor = Strings.isNullOrEmpty(name)
                                            ? this.lookup
                                            : Preconditions.checkNotNull(
                                                lookupReferencesManager.get(name),
                                                "Lookup [%s] not found",
                                                name
                                            ).getLookupExtractorFactory().get();

    return new LookupExtractionFn(
        lookupExtractor,
        retainMissingValue,
        replaceMissingValueWith,
        lookupExtractor.isOneToOne(),
        optimize
    );
  }

  @Override
  public DimensionSelector decorate(DimensionSelector selector)
  {
    return selector;
  }

  @Override
  public boolean mustDecorate()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    byte[] dimExtractionFnBytes = Strings.isNullOrEmpty(name)
                                  ? getLookup().getCacheKey()
                                  : StringUtils.toUtf8(name);
    byte[] outputNameBytes = StringUtils.toUtf8(outputName);
    byte[] replaceWithBytes = StringUtils.toUtf8(Strings.nullToEmpty(replaceMissingValueWith));


    return ByteBuffer.allocate(6
                               + dimensionBytes.length
                               + outputNameBytes.length
                               + dimExtractionFnBytes.length
                               + replaceWithBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(outputNameBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(dimExtractionFnBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(replaceWithBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(retainMissingValue ? (byte) 1 : (byte) 0)
                     .array();
  }

  @Override
  public boolean preservesOrdering()
  {
    return getExtractionFn().preservesOrdering();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LookupDimensionSpec)) {
      return false;
    }

    LookupDimensionSpec that = (LookupDimensionSpec) o;

    if (retainMissingValue != that.retainMissingValue) {
      return false;
    }
    if (optimize != that.optimize) {
      return false;
    }
    if (!getDimension().equals(that.getDimension())) {
      return false;
    }
    if (!getOutputName().equals(that.getOutputName())) {
      return false;
    }
    if (getLookup() != null ? !getLookup().equals(that.getLookup()) : that.getLookup() != null) {
      return false;
    }
    if (replaceMissingValueWith != null
        ? !replaceMissingValueWith.equals(that.replaceMissingValueWith)
        : that.replaceMissingValueWith != null) {
      return false;
    }
    return getName() != null ? getName().equals(that.getName()) : that.getName() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getDimension().hashCode();
    result = 31 * result + getOutputName().hashCode();
    result = 31 * result + (getLookup() != null ? getLookup().hashCode() : 0);
    result = 31 * result + (retainMissingValue ? 1 : 0);
    result = 31 * result + (replaceMissingValueWith != null ? replaceMissingValueWith.hashCode() : 0);
    result = 31 * result + (getName() != null ? getName().hashCode() : 0);
    result = 31 * result + (optimize ? 1 : 0);
    return result;
  }
}
