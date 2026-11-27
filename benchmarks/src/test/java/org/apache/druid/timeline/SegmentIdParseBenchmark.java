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

package org.apache.druid.timeline;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Compares {@link SegmentId#tryParse} (with the fast canonical-timestamp path added for #20927) against a frozen
 * copy of the pre-optimization implementation, which parsed every timestamp with the lenient
 * {@link DateTimes#ISO_DATE_TIME} formatter.
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SegmentIdParseBenchmark
{
  private static final String DATA_SOURCE = "wikipedia";
  private static final char DELIMITER = '_';
  private static final Splitter DELIMITER_SPLITTER = Splitter.on(DELIMITER);
  private static final Joiner DELIMITER_JOINER = Joiner.on(DELIMITER);

  private List<String> idsWithExplicitPartition;
  private List<String> idsWithoutExplicitPartition;
  private List<String> idsWithUnderscoreInVersion;

  @Setup(Level.Trial)
  public void setup()
  {
    idsWithExplicitPartition = new ArrayList<>();
    idsWithoutExplicitPartition = new ArrayList<>();
    idsWithUnderscoreInVersion = new ArrayList<>();

    final DateTime start = DateTimes.of("2025-01-01");
    for (int i = 0; i < 1000; i++) {
      final Interval interval = new Interval(start.plusDays(i), start.plusDays(i + 1));
      idsWithExplicitPartition.add(SegmentId.of(DATA_SOURCE, interval, "2025-01-01T00:00:00.000Z", i + 1).toString());
      idsWithoutExplicitPartition.add(SegmentId.of(DATA_SOURCE, interval, "2025-01-01T00:00:00.000Z", 0).toString());
      idsWithUnderscoreInVersion.add(
          SegmentId.of(DATA_SOURCE, interval, "2025-01-01T00:00:00.000Z_extra_suffix", i + 1).toString()
      );
    }
    idsWithExplicitPartition = Collections.unmodifiableList(idsWithExplicitPartition);
    idsWithoutExplicitPartition = Collections.unmodifiableList(idsWithoutExplicitPartition);
    idsWithUnderscoreInVersion = Collections.unmodifiableList(idsWithUnderscoreInVersion);
  }

  @Benchmark
  public void current_withExplicitPartition(Blackhole blackhole)
  {
    for (String id : idsWithExplicitPartition) {
      blackhole.consume(SegmentId.tryParse(DATA_SOURCE, id));
    }
  }

  @Benchmark
  public void legacy_withExplicitPartition(Blackhole blackhole)
  {
    for (String id : idsWithExplicitPartition) {
      blackhole.consume(legacyTryParse(DATA_SOURCE, id));
    }
  }

  @Benchmark
  public void current_withoutExplicitPartition(Blackhole blackhole)
  {
    for (String id : idsWithoutExplicitPartition) {
      blackhole.consume(SegmentId.tryParse(DATA_SOURCE, id));
    }
  }

  @Benchmark
  public void legacy_withoutExplicitPartition(Blackhole blackhole)
  {
    for (String id : idsWithoutExplicitPartition) {
      blackhole.consume(legacyTryParse(DATA_SOURCE, id));
    }
  }

  @Benchmark
  public void current_versionWithUnderscore(Blackhole blackhole)
  {
    for (String id : idsWithUnderscoreInVersion) {
      blackhole.consume(SegmentId.tryParse(DATA_SOURCE, id));
    }
  }

  @Benchmark
  public void legacy_versionWithUnderscore(Blackhole blackhole)
  {
    for (String id : idsWithUnderscoreInVersion) {
      blackhole.consume(legacyTryParse(DATA_SOURCE, id));
    }
  }

  /**
   * Frozen copy of {@link SegmentId#iteratePossibleParsingsWithDataSource} as it existed before #20927: identical
   * in every respect except that it always calls the lenient {@link DateTimes#ISO_DATE_TIME} formatter directly,
   * never the fast canonical-timestamp path. Kept here only so this benchmark has a stable "before" baseline to
   * compare against, independent of future changes to the production method.
   */
  private static SegmentId legacyTryParse(String dataSource, String segmentId)
  {
    if (!segmentId.startsWith(dataSource) || segmentId.charAt(dataSource.length()) != DELIMITER) {
      return null;
    }

    String remaining = segmentId.substring(dataSource.length() + 1);
    List<String> splits = DELIMITER_SPLITTER.splitToList(remaining);
    if (splits.size() < 3) {
      return null;
    }

    DateTime start;
    DateTime end;
    try {
      start = DateTimes.ISO_DATE_TIME.parse(splits.get(0));
      end = DateTimes.ISO_DATE_TIME.parse(splits.get(1));
    }
    catch (IllegalArgumentException e) {
      return null;
    }
    if (start.compareTo(end) >= 0) {
      return null;
    }

    String version = DELIMITER_JOINER.join(splits.subList(2, Math.max(splits.size() - 1, 3)));
    String trail = splits.size() > 3 ? splits.get(splits.size() - 1) : null;
    int partitionNum = 0;
    if (trail != null) {
      Integer possiblePartitionNum = Ints.tryParse(trail);
      if (possiblePartitionNum != null && possiblePartitionNum > 0) {
        partitionNum = possiblePartitionNum;
      } else {
        version = version + '_' + trail;
      }
    }
    return SegmentId.of(dataSource, new Interval(start, end), version, partitionNum);
  }
}
