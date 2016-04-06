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

package io.druid.segment;

import com.google.common.base.Function;
import com.metamx.common.logger.Logger;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Objects;

/**
 * identifier to DataSegment. wishfully included in DataSegment
 */
public class SegmentDesc
{
  private static final Logger LOGGER = new Logger(SegmentDesc.class);

  public static Function<String, Interval> INTERVAL_EXTRACTOR = new Function<String, Interval>()
  {
    @Override
    public Interval apply(String identifier)
    {
      return valueOf(identifier).getInterval();
    }
  };

  // ignores shard spec
  public static SegmentDesc valueOf(final String identifier)
  {
    SegmentDesc segmentDesc = parse(identifier);
    if (segmentDesc == null) {
      throw new IllegalArgumentException("Invalid identifier " + identifier);
    }
    return segmentDesc;
  }

  private static SegmentDesc parse(String identifier)
  {
    String[] splits = identifier.split(DataSegment.delimiter);
    if (splits.length < 3) {
      return null;
    }

    DateTimeFormatter formatter = ISODateTimeFormat.dateTime();

    String datasource = null;
    DateTime start = null;
    DateTime end = null;

    int i = 1;
    for (; i < splits.length && end == null; i++) {
      try {
        DateTime dateTime = formatter.parseDateTime(splits[i]);
        if (start == null) {
          datasource = StringUtils.join(splits, DataSegment.delimiter, 0, i);
          start = dateTime;
        } else if (end == null) {
          end = dateTime;
        }
      }
      catch (IllegalArgumentException e) {
        // ignore
      }
    }
    if (end == null) {
      return null;
    }

    String version = i < splits.length ? splits[i++] : null;
    String trail = i < splits.length ? StringUtils.join(splits, DataSegment.delimiter, i, splits.length) : null;

    return new SegmentDesc(
        datasource,
        new Interval(start.getMillis(), end.getMillis()),
        version,
        trail
    );
  }

  public static String withInterval(final String identifier, Interval newInterval)
  {
    SegmentDesc segmentDesc = SegmentDesc.parse(identifier);
    if (segmentDesc == null) {
      // happens for test segments which has invalid segment id.. ignore for now
      LOGGER.warn("Invalid segment identifier " + identifier);
      return identifier;
    }
    return segmentDesc.withInterval(newInterval).toString();
  }

  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final String trail;

  public SegmentDesc(String dataSource, Interval interval, String version, String trail)
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

  public SegmentDesc withInterval(Interval interval) {
    return new SegmentDesc(dataSource, interval, version, trail);
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

    SegmentDesc that = (SegmentDesc) o;

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
  public String toString() {
    return StringUtils.join(
        new Object[] {dataSource, interval.getStart(), interval.getEnd(), version, trail},
        DataSegment.delimiter, 0, version == null ? 3 : trail == null ? 4 : 5);
  }
}
