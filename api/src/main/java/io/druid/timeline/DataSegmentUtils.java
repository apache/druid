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
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
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
        SegmentIdentifierParts segmentIdentifierParts = valueOf(datasource, identifier);
        if (segmentIdentifierParts == null) {
          throw new IAE("Invalid identifier [%s]", identifier);
        }

        return segmentIdentifierParts.getInterval();
      }
    };
  }

  /**
   * Parses a segment identifier into its components: dataSource, interval, version, and any trailing tags. Ignores
   * shard spec.
   *
   * It is possible that this method may incorrectly parse an identifier, for example if the dataSource name in the
   * identifier contains a DateTime parseable string such as 'datasource_2000-01-01T00:00:00.000Z' and dataSource was
   * provided as 'datasource'. The desired behavior in this case would be to return null since the identifier does not
   * actually belong to the provided dataSource but a non-null result would be returned. This is an edge case that would
   * currently only affect paged select queries with a union dataSource of two similarly-named dataSources as in the
   * given example.
   *
   * @param dataSource the dataSource corresponding to this identifier
   * @param identifier segment identifier
   * @return a {@link DataSegmentUtils.SegmentIdentifierParts} object if the identifier could be parsed, null otherwise
   */
  public static SegmentIdentifierParts valueOf(String dataSource, String identifier)
  {
    if (!identifier.startsWith(StringUtils.format("%s_", dataSource))) {
      return null;
    }

    String remaining = identifier.substring(dataSource.length() + 1);
    String[] splits = remaining.split(DataSegment.delimiter);
    if (splits.length < 3) {
      return null;
    }

    DateTimeFormatter formatter = ISODateTimeFormat.dateTime();

    try {
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
    catch (IllegalArgumentException e) {
      return null;
    }
  }

  public static String withInterval(final String dataSource, final String identifier, Interval newInterval)
  {
    SegmentIdentifierParts segmentDesc = DataSegmentUtils.valueOf(dataSource, identifier);
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
