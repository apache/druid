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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.data.input.Row;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.IdentityExtractionFn;

import java.nio.ByteBuffer;
import java.util.List;

public class DimensionSelectorHavingSpec implements HavingSpec
{
  private static final byte CACHE_KEY = (byte) 0x8;
  private static final byte STRING_SEPARATOR = (byte) 0xFF;
  private final String dimension;
  private final String value;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public DimensionSelectorHavingSpec(
      @JsonProperty("dimension") String dimName,
      @JsonProperty("value") String value,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    dimension = Preconditions.checkNotNull(dimName, "Must have attribute 'dimension'");
    this.value = value;
    this.extractionFn = extractionFn != null ? extractionFn : IdentityExtractionFn.getInstance();
  }

  @JsonProperty("value")
  public String getValue()
  {
    return value;
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  public boolean eval(Row row)
  {
    List<String> dimRowValList = row.getDimension(dimension);
    if (dimRowValList == null || dimRowValList.isEmpty()) {
      return Strings.isNullOrEmpty(value);
    }

    for (String rowVal : dimRowValList) {
      String extracted = getExtractionFn().apply(rowVal);
      if (value != null && value.equals(extracted)) {
        return true;
      }
      if (extracted == null || extracted.isEmpty()) {
        return Strings.isNullOrEmpty(value);
      }
    }

    return false;
  }

  public byte[] getCacheKey()
  {
    byte[] dimBytes = StringUtils.toUtf8(dimension);
    byte[] valBytes = StringUtils.toUtf8(value);
    byte [] extractionFnBytes = this.getExtractionFn().getCacheKey();

    return ByteBuffer.allocate(3 + dimBytes.length + valBytes.length + extractionFnBytes.length)
                       .put(CACHE_KEY)
                       .put(dimBytes)
                       .put(STRING_SEPARATOR)
                       .put(valBytes)
                       .put(STRING_SEPARATOR)
                       .put(extractionFnBytes)
                       .array();
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

    DimensionSelectorHavingSpec that = (DimensionSelectorHavingSpec) o;
    boolean valEquals = false;
    boolean dimEquals = false;

    if (value != null && that.value != null) {
      valEquals = value.equals(that.value);
    } else if (value == null && that.value == null) {
      valEquals = true;
    }

    if (dimension != null && that.dimension != null) {
      dimEquals = dimension.equals(that.dimension);
    } else if (dimension == null && that.dimension == null) {
      dimEquals = true;
    }

    return (valEquals && dimEquals && extractionFn.equals(that.extractionFn));
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }


  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("DimensionSelectorHavingSpec");
    sb.append("{dimension='").append(dimension).append('\'');
    sb.append(", value='").append(value);
    sb.append("', extractionFunction='").append(getExtractionFn());
    sb.append("'}");
    return sb.toString();
  }

}
