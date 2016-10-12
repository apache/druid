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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class DimsConcatExtractionFn extends MultiInputFunctionalExtraction
{
  private final String format;
  private final String delimiter;

  @JsonCreator
  public DimsConcatExtractionFn(
      @JsonProperty("format") final String format,
      @JsonProperty("delimiter") final String delimiter
  )
  {
    super(
        (format != null) ?
            new Function<List<String>, String>() {
              @Override
              public String apply(List<String> input) {
                return input == null ? "" : String.format(format, input.toArray(new String[input.size()]));
              }
            }
            :
            new Function<List<String>, String>() {
              @Nullable
              @Override
              public String apply(@Nullable List<String> input) {
                return input == null ? "" : StringUtils.join(input, delimiter);
              }
            },
        null
    );

    this.format = format;
    this.delimiter = delimiter;
  }

  @JsonProperty
  public String getFormat()
  {
    return format;
  }

  @JsonProperty
  public String getDelimiter()
  {
    return delimiter;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] formatBytes = (format == null) ? new byte[]{} : io.druid.common.utils.StringUtils.toUtf8(format);
    byte[] delimiterBytes = (delimiter == null) ? new byte[]{} : io.druid.common.utils.StringUtils.toUtf8(delimiter);
    return ByteBuffer.allocate(3 + formatBytes.length + delimiterBytes.length)
        .put(ExtractionCacheHelper.CACHE_TYPE_ID_MULTICONCAT)
        .put(ExtractionCacheHelper.CACHE_KEY_SEPARATOR)
        .put(formatBytes)
        .put(ExtractionCacheHelper.CACHE_KEY_SEPARATOR)
        .put(delimiterBytes)
        .array();
  }

  @Override
  public int arity()
  {
    // negative means varargs
    return -1;
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

    DimsConcatExtractionFn that = (DimsConcatExtractionFn) o;

    if (getFormat() != null ? !getFormat().equals(that.getFormat()) : that.getFormat() != null) {
      return false;
    }
    return getDelimiter() != null ? getDelimiter().equals(that.getDelimiter()) : that.getDelimiter() == null;
  }

  @Override
  public int hashCode()
  {
    int result = getFormat() != null ? getFormat().hashCode() : 0;
    result = 31 * result + getDelimiter() != null ? getDelimiter().hashCode() : 0;

    return result;
  }
}
