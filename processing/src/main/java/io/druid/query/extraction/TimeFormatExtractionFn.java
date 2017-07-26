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
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Locale;

public class TimeFormatExtractionFn implements ExtractionFn
{
  private final String format;
  private final DateTimeZone tz;
  private final Locale locale;
  private final Granularity granularity;
  private final boolean asMillis;
  private final DateTimeFormatter formatter;

  public TimeFormatExtractionFn(
      @JsonProperty("format") String format,
      @JsonProperty("timeZone") DateTimeZone tz,
      @JsonProperty("locale") String localeString,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("asMillis") boolean asMillis
  )
  {
    this.format = format;
    this.tz = tz;
    this.locale = localeString == null ? null : Locale.forLanguageTag(localeString);
    this.granularity = granularity == null ? Granularities.NONE : granularity;

    if (asMillis && format == null) {
      Preconditions.checkArgument(tz == null, "timeZone requires a format");
      Preconditions.checkArgument(localeString == null, "locale requires a format");
      this.formatter = null;
    } else {
      this.formatter = (format == null ? ISODateTimeFormat.dateTime() : DateTimeFormat.forPattern(format))
          .withZone(tz == null ? DateTimeZone.UTC : tz)
          .withLocale(locale);
    }

    this.asMillis = asMillis;
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
  public Granularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public boolean isAsMillis()
  {
    return asMillis;
  }

  @Override
  public byte[] getCacheKey()
  {
    final String tzId = (tz == null ? DateTimeZone.UTC : tz).getID();
    final String localeTag = (locale == null ? Locale.getDefault() : locale).toLanguageTag();
    final byte[] exprBytes = StringUtils.toUtf8(format + "\u0001" + tzId + "\u0001" + localeTag);
    final byte[] granularityCacheKey = granularity.getCacheKey();
    return ByteBuffer.allocate(4 + exprBytes.length + granularityCacheKey.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_TIME_FORMAT)
                     .put(exprBytes)
                     .put((byte) 0xFF)
                     .put(granularityCacheKey)
                     .put((byte) 0xFF)
                     .put(asMillis ? (byte) 1 : (byte) 0)
                     .array();
  }

  @Override
  public String apply(long value)
  {
    final long truncated = granularity.bucketStart(new DateTime(value)).getMillis();
    return formatter == null ? String.valueOf(truncated) : formatter.print(truncated);
  }

  @Override
  @Nullable
  public String apply(@Nullable Object value)
  {
    if (value == null) {
      return null;
    }

    if (asMillis && value instanceof String) {
      final Long theLong = GuavaUtils.tryParseLong((String) value);
      return theLong == null ? apply(new DateTime(value).getMillis()) : apply(theLong.longValue());
    } else {
      return apply(new DateTime(value).getMillis());
    }
  }

  @Override
  @Nullable
  public String apply(@Nullable String value)
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

    if (asMillis != that.asMillis) {
      return false;
    }
    if (format != null ? !format.equals(that.format) : that.format != null) {
      return false;
    }
    if (tz != null ? !tz.equals(that.tz) : that.tz != null) {
      return false;
    }
    if (locale != null ? !locale.equals(that.locale) : that.locale != null) {
      return false;
    }
    return granularity != null ? granularity.equals(that.granularity) : that.granularity == null;

  }

  @Override
  public int hashCode()
  {
    int result = format != null ? format.hashCode() : 0;
    result = 31 * result + (tz != null ? tz.hashCode() : 0);
    result = 31 * result + (locale != null ? locale.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (asMillis ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("timeFormat(\"%s\", %s, %s, %s, %s)", format, tz, locale, granularity, asMillis);
  }
}
