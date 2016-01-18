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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.nio.ByteBuffer;
import java.util.Locale;

public class TimeFormatExtractionFn implements ExtractionFn
{
  private final DateTimeZone tz;
  private final String pattern;
  private final Locale locale;
  private final DateTimeFormatter formatter;

  public TimeFormatExtractionFn(
      @JsonProperty("format") String pattern,
      @JsonProperty("timeZone") DateTimeZone tz,
      @JsonProperty("locale") String localeString
  )
  {
    Preconditions.checkArgument(pattern != null, "format cannot be null");

    this.pattern = pattern;
    this.tz = tz;
    this.locale = localeString == null ? null : Locale.forLanguageTag(localeString);
    this.formatter = DateTimeFormat.forPattern(pattern)
                                   .withZone(tz == null ? DateTimeZone.UTC : tz)
                                   .withLocale(locale);
  }

  @JsonProperty
  public DateTimeZone getTimeZone()
  {
    return tz;
  }

  @JsonProperty
  public String getFormat()
  {
    return pattern;
  }

  @JsonProperty
  public String getLocale()
  {
    if (locale != null) {
      return locale.toLanguageTag();
    } else {
      return null;
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] exprBytes = StringUtils.toUtf8(pattern + "\u0001" + tz.getID() + "\u0001" + locale.toLanguageTag());
    return ByteBuffer.allocate(1 + exprBytes.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_TIME_FORMAT)
                     .put(exprBytes)
                     .array();
  }

  @Override
  public String apply(long value)
  {
    return formatter.print(value);
  }

  @Override
  public String apply(Object value)
  {
    return formatter.print(new DateTime(value));
  }

  @Override
  public String apply(String value)
  {
    return apply((Object) value);
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

    TimeFormatExtractionFn that = (TimeFormatExtractionFn) o;

    if (locale != null ? !locale.equals(that.locale) : that.locale != null) {
      return false;
    }
    if (!pattern.equals(that.pattern)) {
      return false;
    }
    if (tz != null ? !tz.equals(that.tz) : that.tz != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = tz != null ? tz.hashCode() : 0;
    result = 31 * result + pattern.hashCode();
    result = 31 * result + (locale != null ? locale.hashCode() : 0);
    return result;
  }
}
