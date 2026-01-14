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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test utility to create {@link DataSegment}s for a given datasource.
 */
public class CreateDataSegments
{
  private static final DateTime DEFAULT_START = DateTimes.of("2012-10-24");

  private final String datasource;

  private DateTime startTime = DEFAULT_START.plusDays(ThreadLocalRandom.current().nextInt(3000));
  private Granularity granularity = Granularities.DAY;
  private int numPartitions = 1;
  private int numIntervals = 1;
  private long sizeInBytes = 500_000_000;

  private String version = "1";
  private CompactionState compactionState = null;

  // Plus fields
  private Boolean used;
  private DateTime lastUpdatedTime;
  private String upgradedFromSegmentId;
  private String schemaFingerprint;
  private Long numRows;
  private String indexingStateFingerprint;

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

  public CreateDataSegments startingAt(DateTime startOfFirstInterval)
  {
    this.startTime = startOfFirstInterval;
    return this;
  }

  public CreateDataSegments withNumPartitions(int numPartitions)
  {
    this.numPartitions = numPartitions;
    return this;
  }

  public CreateDataSegments withCompactionState(CompactionState compactionState)
  {
    this.compactionState = compactionState;
    return this;
  }

  /**
   * Specifies the version to use for creating the segments. Default version is
   * {@code "1"}.
   */
  public CreateDataSegments withVersion(String version)
  {
    this.version = version;
    return this;
  }

  public CreateDataSegments withNumRows(Long numRows)
  {
    this.numRows = numRows;
    return this;
  }

  public CreateDataSegments withSchemaFingerprint(String schemaFingerprint)
  {
    this.schemaFingerprint = schemaFingerprint;
    return this;
  }

  public CreateDataSegments withIndexingStateFingerprint(String indexingStateFingerprint)
  {
    this.indexingStateFingerprint = indexingStateFingerprint;
    return this;
  }

  public CreateDataSegments markUnused()
  {
    this.used = false;
    return this;
  }

  public CreateDataSegments markUsed()
  {
    this.used = true;
    return this;
  }

  public CreateDataSegments lastUpdatedOn(DateTime updatedTime)
  {
    this.lastUpdatedTime = updatedTime;
    return this;
  }

  public CreateDataSegments updatedNow()
  {
    return lastUpdatedOn(DateTimes.nowUtc());
  }

  /**
   * Creates a single {@link DataSegmentPlus} object with the specified parameters.
   */
  public DataSegmentPlus asPlus()
  {
    return plus(eachOfSize(sizeInBytes).get(0));
  }

  public List<DataSegment> eachOfSizeInMb(long sizeMb)
  {
    return eachOfSize(sizeMb * 1_000_000);
  }

  public List<DataSegment> eachOfSize(long sizeInBytes)
  {
    boolean isEternityInterval = Objects.equals(granularity, Granularities.ALL);
    if (isEternityInterval) {
      numIntervals = 1;
    }

    int uniqueIdInInterval = 0;
    DateTime nextStart = startTime;

    final List<DataSegment> segments = new ArrayList<>();
    for (int numInterval = 0; numInterval < numIntervals; ++numInterval) {
      Interval nextInterval = isEternityInterval
                              ? Intervals.ETERNITY
                              : new Interval(nextStart, granularity.increment(nextStart));
      for (int numPartition = 0; numPartition < numPartitions; ++numPartition) {
        segments.add(
            new NumberedDataSegment(
                datasource,
                nextInterval,
                version,
                new NumberedShardSpec(numPartition, numPartitions),
                ++uniqueIdInInterval,
                compactionState,
                sizeInBytes,
                indexingStateFingerprint
            )
        );
      }
      nextStart = granularity.increment(nextStart);
    }

    return Collections.unmodifiableList(segments);
  }

  private DataSegmentPlus plus(DataSegment segment)
  {
    return new DataSegmentPlus(
        segment,
        DateTimes.nowUtc(),
        lastUpdatedTime,
        used,
        schemaFingerprint,
        numRows,
        upgradedFromSegmentId,
        indexingStateFingerprint
    );
  }

  /**
   * Simple implementation of DataSegment with a unique integer id to make debugging easier.
   */
  private static class NumberedDataSegment extends DataSegment
  {
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyyMMdd");
    private final int uniqueId;

    private NumberedDataSegment(
        String datasource,
        Interval interval,
        String version,
        NumberedShardSpec shardSpec,
        int uniqueId,
        CompactionState compactionState,
        long size,
        String indexingStateFingerprint
    )
    {
      super(
          datasource,
          interval,
          version,
          Collections.emptyMap(),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList(),
          shardSpec,
          compactionState,
          IndexIO.CURRENT_VERSION_ID,
          size,
          indexingStateFingerprint,
          PruneSpecsHolder.DEFAULT
      );
      this.uniqueId = uniqueId;
    }

    @Override
    public String toString()
    {
      return "{" + getDataSource()
             + "::" + getInterval().getStart().toString(FORMATTER)
             + "::" + uniqueId + "}";
    }
  }
}
