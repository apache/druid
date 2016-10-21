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

package io.druid.timeline;

import com.google.common.base.Function;

import io.druid.java.util.common.logger.Logger;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Objects;

/**
 * identifier to DataSegment.
 */
public class DataSegmentUtils
{
  private static final Logger LOGGER = new Logger(DataSegmentUtils.class);

  public static Function<String, Interval> INTERVAL_EXTRACTOR(final String datasource)
  {
    return new Function<String, Interval>()
    {
      @Override
      public Interval apply(String identifier)
      {
        return valueOf(datasource, identifier).getInterval();
      }
    };
  }

  // ignores shard spec
  public static SegmentIdentifierParts valueOf(String dataSource, String identifier)
  {
    SegmentIdentifierParts segmentDesc = parse(dataSource, identifier);
    if (segmentDesc == null) {
      throw new IllegalArgumentException("Invalid identifier " + identifier);
    }
    return segmentDesc;
  }

  private static SegmentIdentifierParts parse(String dataSource, String identifier)
  {
    if (!identifier.startsWith(String.format("%s_", dataSource))) {
      LOGGER.info("Invalid identifier %s", identifier);
      return null;
    }
    String remaining = identifier.substring(dataSource.length() + 1);
    String[] splits = remaining.split(DataSegment.delimiter);
    if (splits.length < 3) {
      LOGGER.info("Invalid identifier %s", identifier);
      return null;
    }

    DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
    DateTime start = formatter.parseDateTime(splits[0]);
    DateTime end = formatter.parseDateTime(splits[1]);
    String version = splits[2];
    String trail = splits.length > 3 ? join(splits, DataSegment.delimiter, 3, splits.length) : null;

    return new SegmentIdentifierParts(
        dataSource,
        new Interval(start.getMillis(), end.getMillis()),
        version,
        trail
    );
  }

  public static String withInterval(final String dataSource, final String identifier, Interval newInterval)
  {
    SegmentIdentifierParts segmentDesc = DataSegmentUtils.parse(dataSource, identifier);
    if (segmentDesc == null) {
      // happens for test segments which has invalid segment id.. ignore for now
      LOGGER.warn("Invalid segment identifier " + identifier);
      return identifier;
    }
    return segmentDesc.withInterval(newInterval).toString();
  }

  static class SegmentIdentifierParts
  {
    private final String dataSource;
    private final Interval interval;
    private final String version;
    private final String trail;

    public SegmentIdentifierParts(String dataSource, Interval interval, String version, String trail)
    {
      this.dataSource = dataSource;
      this.interval = interval;
      this.version = version;
      this.trail = trail;
    }

    public String getDataSource()
    {
      return dataSource;
    }

    public Interval getInterval()
    {
      return interval;
    }

    public String getVersion()
    {
      return version;
    }

    public SegmentIdentifierParts withInterval(Interval interval)
    {
      return new SegmentIdentifierParts(dataSource, interval, version, trail);
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

      SegmentIdentifierParts that = (SegmentIdentifierParts) o;

      if (!Objects.equals(dataSource, that.dataSource)) {
        return false;
      }
      if (!Objects.equals(interval, that.interval)) {
        return false;
      }
      if (!Objects.equals(version, that.version)) {
        return false;
      }
      if (!Objects.equals(trail, that.trail)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(dataSource, interval, version, trail);
    }

    @Override
    public String toString()
    {
      return join(
          new Object[]{dataSource, interval.getStart(), interval.getEnd(), version, trail},
          DataSegment.delimiter, 0, version == null ? 3 : trail == null ? 4 : 5
      );
    }
  }

  private static String join(Object[] input, String delimiter, int start, int end)
  {
    StringBuilder builder = new StringBuilder();
    for (int i = start; i < end; i++) {
      if (i > start) {
        builder.append(delimiter);
      }
      if (input[i] != null) {
        builder.append(input[i]);
      }
    }
    return builder.toString();
  }
}
