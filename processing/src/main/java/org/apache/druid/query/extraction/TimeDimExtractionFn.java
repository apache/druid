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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.ibm.icu.text.SimpleDateFormat;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 */
public class TimeDimExtractionFn extends DimExtractionFn
{
  private final String timeFormat;
  private final String resultFormat;
  private final Supplier<Function<String, String>> fn;
  private final boolean joda;

  @JsonCreator
  public TimeDimExtractionFn(
      @JsonProperty("timeFormat") String timeFormat,
      @JsonProperty("resultFormat") String resultFormat,
      @JsonProperty("joda") boolean joda
  )
  {
    Preconditions.checkNotNull(timeFormat, "timeFormat must not be null");
    Preconditions.checkNotNull(resultFormat, "resultFormat must not be null");

    this.joda = joda;
    this.timeFormat = timeFormat;
    this.resultFormat = resultFormat;
    this.fn = makeFunctionSupplier();
  }

  private Supplier<Function<String, String>> makeFunctionSupplier()
  {
    if (joda) {
      final DateTimes.UtcFormatter parser = DateTimes.wrapFormatter(DateTimeFormat.forPattern(timeFormat));
      final DateTimes.UtcFormatter formatter = DateTimes.wrapFormatter(DateTimeFormat.forPattern(resultFormat));

      final Function<String, String> fn = value -> {
        DateTime date;
        try {
          date = parser.parse(value);
        }
        catch (IllegalArgumentException e) {
          return value;
        }
        return formatter.print(date);
      };

      // Single shared function, since Joda formatters are thread-safe.
      return () -> fn;
    } else {
      final ThreadLocal<Function<String, String>> threadLocal = ThreadLocal.withInitial(
          () -> {
            final SimpleDateFormat parser = new SimpleDateFormat(timeFormat, Locale.ENGLISH);
            final SimpleDateFormat formatter = new SimpleDateFormat(resultFormat, Locale.ENGLISH);
            parser.setLenient(true);

            return value -> {
              Date date;
              try {
                date = parser.parse(value);
              }
              catch (ParseException e) {
                return value;
              }
              return formatter.format(date);
            };
          }
      );

      // Thread-local, since SimpleDateFormats are not thread-safe.
      return threadLocal::get;
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] timeFormatBytes = StringUtils.toUtf8(timeFormat);
    return ByteBuffer.allocate(1 + timeFormatBytes.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_TIME_DIM)
                     .put(timeFormatBytes)
                     .array();
  }

  @Nullable
  @Override
  public String apply(@Nullable String dimValue)
  {
    if (NullHandling.isNullOrEquivalent(dimValue)) {
      return null;
    }

    return fn.get().apply(dimValue);
  }

  @JsonProperty
  public String getTimeFormat()
  {
    return timeFormat;
  }

  @JsonProperty
  public String getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty
  public boolean isJoda()
  {
    return joda;
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
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TimeDimExtractionFn that = (TimeDimExtractionFn) o;
    return joda == that.joda &&
           Objects.equals(timeFormat, that.timeFormat) &&
           Objects.equals(resultFormat, that.resultFormat);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timeFormat, resultFormat, joda);
  }

  @Override
  public String toString()
  {
    return "TimeDimExtractionFn{" +
           "timeFormat='" + timeFormat + '\'' +
           ", resultFormat='" + resultFormat + '\'' +
           ", joda=" + joda +
           '}';
  }
}
