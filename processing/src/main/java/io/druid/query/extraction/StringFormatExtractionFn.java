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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 *
 */
public class StringFormatExtractionFn extends DimExtractionFn
{
  public static enum NullHandling
  {
    NULLSTRING,
    EMPTYSTRING,
    RETURNNULL;

    @JsonCreator
    public static NullHandling forValue(String value)
    {
      return value == null ? NULLSTRING : NullHandling.valueOf(StringUtils.toUpperCase(value));
    }

    @JsonValue
    public String toValue()
    {
      return StringUtils.toLowerCase(name());
    }
  }

  private final String format;
  private final NullHandling nullHandling;

  @JsonCreator
  public StringFormatExtractionFn(
      @JsonProperty("format") String format,
      @JsonProperty("nullHandling") NullHandling nullHandling
  )
  {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(format), "format string should not be empty");
    this.format = format;
    this.nullHandling = nullHandling == null ? NullHandling.NULLSTRING : nullHandling;
  }

  public StringFormatExtractionFn(String format)
  {
    this(format, NullHandling.NULLSTRING);
  }

  @JsonProperty
  public String getFormat()
  {
    return format;
  }

  @JsonProperty
  public NullHandling getNullHandling()
  {
    return nullHandling;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] bytes = StringUtils.toUtf8(format);
    return ByteBuffer.allocate(2 + bytes.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_STRING_FORMAT)
                     .put((byte) nullHandling.ordinal())
                     .put(bytes)
                     .array();
  }

  @Nullable
  @Override
  public String apply(@Nullable String value)
  {
    if (value == null) {
      if (nullHandling == NullHandling.RETURNNULL) {
        return null;
      }
      if (nullHandling == NullHandling.EMPTYSTRING) {
        value = "";
      }
    }
    return Strings.emptyToNull(StringUtils.format(format, value));
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

    if (nullHandling != that.nullHandling) {
      return false;
    }
    return format.equals(that.format);
  }

  @Override
  public int hashCode()
  {
    int result = format.hashCode();
    result = 31 * result + nullHandling.ordinal();
    return result;
  }

  @Override
  public String toString()
  {
    return "StringFormatExtractionFn{" +
           "format='" + format + '\'' +
           ", nullHandling=" + nullHandling +
           '}';
  }
}
