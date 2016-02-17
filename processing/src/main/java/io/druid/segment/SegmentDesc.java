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
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 * identifier to DataSegment. wishfully included in DataSegment
 */
public class SegmentDesc
{
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
    String[] splits = identifier.split(DataSegment.delimiter);
    if (splits.length < 4) {
      throw new IllegalArgumentException("Invalid identifier " + identifier);
    }
    String datasource = splits[0];
    DateTime start = new DateTime(splits[1]);
    DateTime end = new DateTime(splits[2]);
    String version = splits[3];

    return new SegmentDesc(
        datasource,
        new Interval(start.getMillis(), end.getMillis()),
        version
    );
  }

  private final String dataSource;
  private final Interval interval;
  private final String version;

  public SegmentDesc(String dataSource, Interval interval, String version)
  {
    this.dataSource = dataSource;
    this.interval = interval;
    this.version = version;
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
}
