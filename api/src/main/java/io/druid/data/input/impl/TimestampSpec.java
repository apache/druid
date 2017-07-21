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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;

import io.druid.java.util.common.parsers.TimestampParser;

import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class TimestampSpec
{
  private static class ParseCtx
  {
    Object lastTimeObject = null;
    DateTime lastDateTime = null;
  }

  private static final String DEFAULT_COLUMN = "timestamp";
  private static final String DEFAULT_FORMAT = "auto";
  private static final DateTime DEFAULT_MISSING_VALUE = null;

  private final String timestampColumn;
  private final String timestampFormat;
  // this value should never be set for production data
  private final DateTime missingValue;
  /** This field is a derivative of {@link #timestampFormat}; not checked in {@link #equals} and {@link #hashCode} */
  private final Function<Object, DateTime> timestampConverter;

  // remember last value parsed
  private transient ParseCtx parseCtx = new ParseCtx();

  @JsonCreator
  public TimestampSpec(
      @JsonProperty("column") String timestampColumn,
      @JsonProperty("format") String format,
      // this value should never be set for production data
      @JsonProperty("missingValue") DateTime missingValue
  )
  {
    this.timestampColumn = (timestampColumn == null) ? DEFAULT_COLUMN : timestampColumn;
    this.timestampFormat = format == null ? DEFAULT_FORMAT : format;
    this.timestampConverter = TimestampParser.createObjectTimestampParser(timestampFormat);
    this.missingValue = missingValue == null
                        ? DEFAULT_MISSING_VALUE
                        : missingValue;
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

  @JsonProperty("missingValue")
  public DateTime getMissingValue()
  {
    return missingValue;
  }

  public DateTime extractTimestamp(Map<String, Object> input)
  {
    return parseDateTime(input.get(timestampColumn));
  }

  public DateTime parseDateTime(Object input)
  {
    DateTime extracted = missingValue;
    if (input != null) {
      // Check if the input is equal to the last input, so we don't need to parse it again
      if (input.equals(parseCtx.lastTimeObject)) {
        extracted = parseCtx.lastDateTime;
      } else {
        ParseCtx newCtx = new ParseCtx();
        newCtx.lastTimeObject = input;
        extracted = timestampConverter.apply(input);
        newCtx.lastDateTime = extracted;
        parseCtx = newCtx;
      }
    }
    return extracted;
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
    return !(missingValue != null ? !missingValue.equals(that.missingValue) : that.missingValue != null);

  }

  @Override
  public int hashCode()
  {
    int result = timestampColumn.hashCode();
    result = 31 * result + timestampFormat.hashCode();
    result = 31 * result + (missingValue != null ? missingValue.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "TimestampSpec{" +
           "timestampColumn='" + timestampColumn + '\'' +
           ", timestampFormat='" + timestampFormat + '\'' +
           ", missingValue=" + missingValue +
           '}';
  }

  //simple merge strategy on timestampSpec that checks if all are equal or else
  //returns null. this can be improved in future but is good enough for most use-cases.
  public static TimestampSpec mergeTimestampSpec(List<TimestampSpec> toMerge)
  {
    if (toMerge == null || toMerge.size() == 0) {
      return null;
    }

    TimestampSpec result = toMerge.get(0);
    for (int i = 1; i < toMerge.size(); i++) {
      if (toMerge.get(i) == null) {
        continue;
      }
      if (!Objects.equals(result, toMerge.get(i))) {
        return null;
      }
    }

    return result;
  }
}
