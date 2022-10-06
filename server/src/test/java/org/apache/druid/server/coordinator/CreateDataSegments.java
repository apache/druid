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

package org.apache.druid.server.coordinator;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test utility to create {@link DataSegment}s for a given datasource.
 */
public class CreateDataSegments
{
  private final String datasource;

  private DateTime startTime;
  private Granularity granularity;
  private int numPartitions;
  private int numIntervals;

  public static CreateDataSegments ofDatasource(String datasource)
  {
    return new CreateDataSegments(datasource);
  }

  private CreateDataSegments(String datasource)
  {
    this.datasource = datasource;
  }

  public CreateDataSegments forIntervals(int numIntervals, Granularity intervalSize)
  {
    this.numIntervals = numIntervals;
    this.granularity = intervalSize;
    return this;
  }

  public CreateDataSegments startingAt(String startOfFirstInterval)
  {
    this.startTime = DateTimes.of(startOfFirstInterval);
    return this;
  }

  public CreateDataSegments withNumPartitions(int numPartitions)
  {
    this.numPartitions = numPartitions;
    return this;
  }

  public List<DataSegment> eachOfSizeInMb(long sizeMb)
  {
    final List<DataSegment> segments = new ArrayList<>();

    int uniqueIdInInterval = 0;
    DateTime nextStart = startTime;
    for (int numInterval = 0; numInterval < numIntervals; ++numInterval) {
      Interval nextInterval = new Interval(nextStart, granularity.increment(nextStart));
      for (int numPartition = 0; numPartition < numPartitions; ++numPartition) {
        segments.add(
            new NumberedDataSegment(
                datasource,
                nextInterval,
                new NumberedShardSpec(numPartition, numPartitions),
                ++uniqueIdInInterval,
                sizeMb
            )
        );
      }
      nextStart = granularity.increment(nextStart);
    }

    return Collections.unmodifiableList(segments);
  }

  /**
   * Simple implementation of DataSegment with a unique integer id to make debugging easier.
   */
  private static class NumberedDataSegment extends DataSegment
  {
    private final int uniqueId;

    private NumberedDataSegment(
        String datasource,
        Interval interval,
        NumberedShardSpec shardSpec,
        int uinqueId,
        long size
    )
    {
      super(
          datasource,
          interval,
          "1",
          Collections.emptyMap(),
          Collections.emptyList(),
          Collections.emptyList(),
          shardSpec,
          IndexIO.CURRENT_VERSION_ID,
          size
      );
      this.uniqueId = uinqueId;
    }

    @Override
    public String toString()
    {
      return "{" + getDataSource() + "::" + uniqueId + "}";
    }
  }
}
