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

package org.apache.druid.data.input;

import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Input row backed by a {@link List}. This implementation is most useful when the columns and dimensions are known
 * ahead of time, prior to encountering any data. It is used in concert with {@link ListBasedInputRowAdapter}, which
 * enables
 */
public class ListBasedInputRow implements InputRow
{
  private final RowSignature signature;
  private final DateTime timestamp;
  private final List<String> dimensions;
  private final List<Object> data;

  /**
   * Create a row.
   *
   * @param signature  signature; must match the "data" list
   * @param timestamp  row timestamp
   * @param dimensions dimensions to be reported by {@link #getDimensions()}
   * @param data       row data
   */
  public ListBasedInputRow(
      final RowSignature signature,
      final DateTime timestamp,
      final List<String> dimensions,
      final List<Object> data
  )
  {
    this.signature = signature;
    this.timestamp = timestamp;
    this.dimensions = dimensions;
    this.data = data;
  }

  /**
   * Create a row and parse a timestamp. Throws {@link ParseException} if the timestamp cannot be parsed.
   *
   * @param signature     signature; must match the "data" list
   * @param timestampSpec timestamp spec
   * @param dimensions    dimensions to be reported by {@link #getDimensions()}
   * @param data          row data
   */
  public static InputRow parse(
      RowSignature signature,
      TimestampSpec timestampSpec,
      List<String> dimensions,
      List<Object> data
  )
  {
    final DateTime timestamp = parseTimestamp(timestampSpec, data, signature);
    return new ListBasedInputRow(signature, timestamp, dimensions, data);
  }

  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return timestamp.getMillis();
  }

  @Override
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    return Rows.objectToStrings(getRaw(dimension));
  }

  @Nullable
  @Override
  public Object getRaw(String columnName)
  {
    final int i = signature.indexOf(columnName);

    if (i < 0 || i >= data.size()) {
      return null;
    } else {
      return data.get(i);
    }
  }

  @Nullable
  public Object getRaw(int columnNumber)
  {
    if (columnNumber < data.size()) {
      return data.get(columnNumber);
    } else {
      // Row may have ended early, which is OK.
      return null;
    }
  }

  @Nullable
  @Override
  public Number getMetric(String metric)
  {
    return Rows.objectToNumber(metric, getRaw(metric), true);
  }

  @Override
  public int compareTo(Row o)
  {
    return timestamp.compareTo(o.getTimestamp());
  }

  public Map<String, Object> asMap()
  {
    return Utils.zipMapPartial(signature.getColumnNames(), data);
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
    ListBasedInputRow that = (ListBasedInputRow) o;
    return Objects.equals(dimensions, that.dimensions)
           && Objects.equals(signature, that.signature)
           && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensions, signature, data);
  }

  @Override
  public String toString()
  {
    return "{" +
           "timestamp=" + DateTimes.utc(getTimestampFromEpoch()) +
           ", event=" + asMap() +
           ", dimensions=" + dimensions +
           '}';
  }

  /**
   * Helper for {@link #parse(RowSignature, TimestampSpec, List, List)}.
   */
  private static DateTime parseTimestamp(
      final TimestampSpec timestampSpec,
      final List<Object> theList,
      final RowSignature signature
  )
  {
    final int timeColumnIndex = signature.indexOf(timestampSpec.getTimestampColumn());
    final Object timeValue;

    if (theList != null && timeColumnIndex >= 0 && timeColumnIndex < theList.size()) {
      timeValue = theList.get(timeColumnIndex);
    } else {
      timeValue = null;
    }

    return MapInputRowParser.parseTimestampOrThrowParseException(
        timeValue,
        timestampSpec,
        () -> Utils.zipMapPartial(signature.getColumnNames(), theList)
    );
  }
}
