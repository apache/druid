/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.metamx.common.parsers.TimestampParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Map;

/**
 */
public class TimestampSpec
{
  private static final String DEFAULT_COLUMN = "timestamp";
  private static final String DEFAULT_FORMAT = "auto";
  private static final DateTime DEFAULT_MISSING_VALUE = null;

  private final String timestampColumn;
  private final String timestampFormat;
  private final Function<Object, DateTime> timestampConverter;
  // this value should never be set for production data
  private final DateTime missingValue;
  // when timestamp column doesn't contain timezone information, such as 'yyyy-MM-dd HH:mm:ss',
  // user can specify it explicitly using "timeZone" argument.
  // note that it's not needed for posix time or timestamp with timezone like 'yyyy-MM-dd HH:mm:ssZZ'
  private final String timeZone;

  private static class EnforceSourceTimeZoneConverter implements Function<Object, DateTime>
  {
    private final Function<Object, DateTime> wrapped;
    private final DateTimeZone timeZone;

    private EnforceSourceTimeZoneConverter(
        Function<Object, DateTime> wrapped,
        DateTimeZone timeZone
    )
    {
      this.wrapped = wrapped;
      this.timeZone = timeZone;
    }

    @Override
    public DateTime apply(Object input)
    {
      DateTime dt = wrapped.apply(input);
      return dt == null ? null : dt.withZoneRetainFields(timeZone);
    }
  }

  public TimestampSpec(
      String timestampColumn,
      String format,
      DateTime missingValue
  )
  {
    this(timestampColumn, format, missingValue, null);
  }

  @JsonCreator
  public TimestampSpec(
      @JsonProperty("column") String timestampColumn,
      @JsonProperty("format") String format,
      // this value should never be set for production data
      @JsonProperty("missingValue") DateTime missingValue,
      @JsonProperty("timeZone") String timeZone
      )
  {
    this.timestampColumn = (timestampColumn == null) ? DEFAULT_COLUMN : timestampColumn;
    this.timestampFormat = format == null ? DEFAULT_FORMAT : format;
    this.timeZone = timeZone;
    this.missingValue = missingValue == null
                        ? DEFAULT_MISSING_VALUE
                        : missingValue;

    Function<Object, DateTime> converter = TimestampParser.createObjectTimestampParser(timestampFormat);
    if (timeZone != null) {
      this.timestampConverter = new EnforceSourceTimeZoneConverter(converter, DateTimeZone.forID(timeZone));
    } else {
      this.timestampConverter = converter;
    }
  }

  @JsonProperty("column")
  public String getTimestampColumn()
  {
    return timestampColumn;
  }

  @JsonProperty("format")
  public String getTimestampFormat()
  {
    return timestampFormat;
  }

  @JsonProperty("timeZone")
  public String getTimeZone()
  {
    return timeZone;
  }

  @JsonProperty("missingValue")
  public DateTime getMissingValue()
  {
    return missingValue;
  }

  public DateTime extractTimestamp(Map<String, Object> input)
  {
    final Object o = input.get(timestampColumn);

    return o == null ? missingValue : timestampConverter.apply(o);
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

    TimestampSpec that = (TimestampSpec) o;

    if (!timestampColumn.equals(that.timestampColumn)) {
      return false;
    }
    if (!timestampFormat.equals(that.timestampFormat)) {
      return false;
    }
    if (missingValue != null ? !missingValue.equals(that.missingValue) : that.missingValue != null) {
      return false;
    }
    return timeZone != null ? timeZone.equals(that.timeZone) : that.timeZone == null;

  }

  @Override
  public int hashCode()
  {
    int result = timestampColumn.hashCode();
    result = 31 * result + timestampFormat.hashCode();
    result = 31 * result + (missingValue != null ? missingValue.hashCode() : 0);
    result = 31 * result + (timeZone != null ? timeZone.hashCode() : 0);
    return result;
  }
}
