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
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.nio.ByteBuffer;
import java.util.Locale;

public class TimeFormatExtractionFn implements ExtractionFn
{
  private final DateTimeZone tz;
  private final String format;
  private final Locale locale;
  private final QueryGranularity granularity;
  private final DateTimeFormatter formatter;

  public TimeFormatExtractionFn(
      @JsonProperty("format") String format,
      @JsonProperty("timeZone") DateTimeZone tz,
      @JsonProperty("locale") String localeString,
      @JsonProperty("granularity") QueryGranularity granularity
  )
  {
    this.format = format;
    this.tz = tz;
    this.locale = localeString == null ? null : Locale.forLanguageTag(localeString);
    this.granularity = granularity == null ? QueryGranularities.NONE : granularity;
    this.formatter = (format == null ? ISODateTimeFormat.dateTime() : DateTimeFormat.forPattern(format))
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
    return format;
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

  @JsonProperty
  public QueryGranularity getGranularity()
  {
    return granularity;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] exprBytes = StringUtils.toUtf8(format + "\u0001" + tz.getID() + "\u0001" + locale.toLanguageTag());
    final byte[] granularityCacheKey = granularity.cacheKey();
    return ByteBuffer.allocate(2 + exprBytes.length + granularityCacheKey.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_TIME_FORMAT)
                     .put(exprBytes)
                     .put((byte) 0xFF)
                     .put(granularityCacheKey)
                     .array();
  }

  @Override
  public String apply(long value)
  {
    return formatter.print(granularity.truncate(value));
  }

  @Override
  public String apply(Object value)
  {
    return apply(new DateTime(value).getMillis());
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

    if (tz != null ? !tz.equals(that.tz) : that.tz != null) {
      return false;
    }
    if (format != null ? !format.equals(that.format) : that.format != null) {
      return false;
    }
    if (locale != null ? !locale.equals(that.locale) : that.locale != null) {
      return false;
    }
    return granularity.equals(that.granularity);

  }

  @Override
  public int hashCode()
  {
    int result = tz != null ? tz.hashCode() : 0;
    result = 31 * result + (format != null ? format.hashCode() : 0);
    result = 31 * result + (locale != null ? locale.hashCode() : 0);
    result = 31 * result + granularity.hashCode();
    return result;
  }
}
