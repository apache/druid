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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;

/**
 *
 */
public class StringFormatExtractionFn extends DimExtractionFn
{
  private final String format;

  @JsonCreator
  public StringFormatExtractionFn(
      @JsonProperty("format") String format
  )
  {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(format), "format string should not be empty");
    this.format = format;
  }

  @JsonProperty
  public String getFormat()
  {
    return format;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] bytes = StringUtils.toUtf8(format);
    return ByteBuffer.allocate(1 + bytes.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_STRING_FORMAT)
                     .put(bytes)
                     .array();
  }

  @Override
  public String apply(String value)
  {
    return String.format(format, value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.MANY_TO_ONE;
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

    StringFormatExtractionFn that = (StringFormatExtractionFn) o;

    return format.equals(that.format);

  }

  @Override
  public int hashCode()
  {
    return format.hashCode();
  }
}
