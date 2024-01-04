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

package org.apache.druid.segment.realtime.appenderator;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.plumber.Committers;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamAppenderatorTest extends InitializedNullHandlingTest
{
  private static final List<SegmentIdWithShardSpec> IDENTIFIERS = ImmutableList.of(
      si("2000/2001", "A", 0),
      si("2000/2001", "A", 1),
      si("2001/2002", "A", 0)
  );

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSimpleIngestion() throws Exception
  {
    try (final StreamAppenderatorTester tester =
             new StreamAppenderatorTester.Builder().maxRowsInMemory(2)
                                                   .enablePushFailure(true)
                                                   .basePersistDirectory(temporaryFolder.newFolder())
                                                   .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      boolean thrown;

      final ConcurrentMap<String, String> commitMetadata = new ConcurrentHashMap<>();
      final Supplier<Committer> committerSupplier = committerSupplierFromConcurrentMap(commitMetadata);

      // startJob
      Assert.assertEquals(null, appenderator.startJob());

      // getDataSource
      Assert.assertEquals(StreamAppenderatorTester.DATASOURCE, appenderator.getDataSource());

      // add
      commitMetadata.put("x", "1");
      Assert.assertEquals(
          1,
          appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier)
                      .getNumRowsInSegment()
      );

      commitMetadata.put("x", "2");
      Assert.assertEquals(
          2,
          appenderator.add(IDENTIFIERS.get(0), ir("2000", "bar", 2), committerSupplier)
                      .getNumRowsInSegment()
      );

      commitMetadata.put("x", "3");
      Assert.assertEquals(
          1,
          appenderator.add(IDENTIFIERS.get(1), ir("2000", "qux", 4), committerSupplier)
                      .getNumRowsInSegment()
      );

      // getSegments
      Assert.assertEquals(IDENTIFIERS.subList(0, 2), sorted(appenderator.getSegments()));

      // getRowCount
      Assert.assertEquals(2, appenderator.getRowCount(IDENTIFIERS.get(0)));
      Assert.assertEquals(1, appenderator.getRowCount(IDENTIFIERS.get(1)));
      thrown = false;
      try {
        appenderator.getRowCount(IDENTIFIERS.get(2));
      }
      catch (IllegalStateException e) {
        thrown = true;
      }
      Assert.assertTrue(thrown);

      // push all
      final SegmentsAndCommitMetadata segmentsAndCommitMetadata = appenderator.push(
          appenderator.getSegments(),
          committerSupplier.get(),
          false
      ).get();
      Assert.assertEquals(
          ImmutableMap.of("x", "3"),
          (Map<String, String>) segmentsAndCommitMetadata.getCommitMetadata()
      );
      Assert.assertEquals(
          IDENTIFIERS.subList(0, 2),
          sorted(
              Lists.transform(
                  segmentsAndCommitMetadata.getSegments(),
                  new Function<DataSegment, SegmentIdWithShardSpec>()
                  {
                    @Override
                    public SegmentIdWithShardSpec apply(DataSegment input)
                    {
                      return SegmentIdWithShardSpec.fromDataSegment(input);
                    }
                  }
              )
          )
      );
      Assert.assertEquals(sorted(tester.getPushedSegments()), sorted(segmentsAndCommitMetadata.getSegments()));

      // clear
      appenderator.clear();
      Assert.assertTrue(appenderator.getSegments().isEmpty());
    }
  }

  @Test
  public void testMaxBytesInMemoryWithSkipBytesInMemoryOverheadCheckConfig() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(100)
                                                  .maxSizeInBytes(1024)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .skipBytesInMemoryOverheadCheck(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of(eventCount, eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            //Do nothing
          }
        };
      };

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      //expectedSizeInBytes = 44(map overhead) + 28 (TimeAndDims overhead) + 56 (aggregator metrics) + 54 (dimsKeySize) = 182 + 1 byte when null handling is enabled
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      Assert.assertEquals(
          182 + nullHandlingOverhead,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar", 1), committerSupplier);
      Assert.assertEquals(
          182 + nullHandlingOverhead,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      appenderator.close();
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
    }
  }

  @Test
  public void testMaxBytesInMemoryInMultipleSinksWithSkipBytesInMemoryOverheadCheckConfig() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(100)
                                                  .maxSizeInBytes(1024)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .skipBytesInMemoryOverheadCheck(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of(eventCount, eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            //Do nothing
          }
        };
      };

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      //expectedSizeInBytes = 44(map overhead) + 28 (TimeAndDims overhead) + 56 (aggregator metrics) + 54 (dimsKeySize) = 182
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      Assert.assertEquals(182 + nullHandlingOverhead, ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory());
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar", 1), committerSupplier);
      Assert.assertEquals(
          364 + 2 * nullHandlingOverhead,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      appenderator.close();
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
    }
  }

  @Test
  public void testMaxBytesInMemory() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(100)
                                                  .maxSizeInBytes(15000)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of(eventCount, eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            //Do nothing
          }
        };
      };

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      // Still under maxSizeInBytes after the add. Hence, we do not persist yet
      //expectedSizeInBytes = 44(map overhead) + 28 (TimeAndDims overhead) + 56 (aggregator metrics) + 54 (dimsKeySize) = 182 + 1 byte when null handling is enabled
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      int currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      int sinkSizeOverhead = 1 * StreamAppenderator.ROUGH_OVERHEAD_PER_SINK;
      // currHydrant in the sink still has > 0 bytesInMemory since we do not persist yet
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assert.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // We do multiple more adds to the same sink to cause persist.
      for (int i = 0; i < 53; i++) {
        appenderator.add(IDENTIFIERS.get(0), ir("2000", "bar_" + i, 1), committerSupplier);
      }
      sinkSizeOverhead = 1 * StreamAppenderator.ROUGH_OVERHEAD_PER_SINK;
      // currHydrant size is 0 since we just persist all indexes to disk.
      currentInMemoryIndexSize = 0;
      // We are now over maxSizeInBytes after the add. Hence, we do a persist.
      // currHydrant in the sink has 0 bytesInMemory since we just did a persist
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      // Mapped index size is the memory still needed after we persisted indexes. Note that the segments have
      // 1 dimension columns, 2 metric column, 1 time column.
      int mappedIndexSize = 1012 + (2 * StreamAppenderator.ROUGH_OVERHEAD_PER_METRIC_COLUMN_HOLDER) +
                            StreamAppenderator.ROUGH_OVERHEAD_PER_DIMENSION_COLUMN_HOLDER +
                            StreamAppenderator.ROUGH_OVERHEAD_PER_TIME_COLUMN_HOLDER;
      Assert.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead + mappedIndexSize,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // Add a single row after persisted
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "bob", 1), committerSupplier);
      // currHydrant in the sink still has > 0 bytesInMemory since we do not persist yet
      currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assert.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead + mappedIndexSize,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // We do multiple more adds to the same sink to cause persist.
      for (int i = 0; i < 31; i++) {
        appenderator.add(IDENTIFIERS.get(0), ir("2000", "bar_" + i, 1), committerSupplier);
      }
      // currHydrant size is 0 since we just persist all indexes to disk.
      currentInMemoryIndexSize = 0;
      // We are now over maxSizeInBytes after the add. Hence, we do a persist.
      // currHydrant in the sink has 0 bytesInMemory since we just did a persist
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      // Mapped index size is the memory still needed after we persisted indexes. Note that the segments have
      // 1 dimension columns, 2 metric column, 1 time column. However, we have two indexes now from the two pervious
      // persists.
      mappedIndexSize = 2 * (1012 + (2 * StreamAppenderator.ROUGH_OVERHEAD_PER_METRIC_COLUMN_HOLDER) +
                             StreamAppenderator.ROUGH_OVERHEAD_PER_DIMENSION_COLUMN_HOLDER +
                             StreamAppenderator.ROUGH_OVERHEAD_PER_TIME_COLUMN_HOLDER);
      Assert.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead + mappedIndexSize,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      appenderator.close();
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory());
    }
  }

  @Test(expected = RuntimeException.class)
  public void testTaskFailAsPersistCannotFreeAnyMoreMemory() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(100)
                                                  .maxSizeInBytes(5180)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of(eventCount, eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            //Do nothing
          }
        };
      };

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
    }
  }

  @Test
  public void testTaskDoesNotFailAsExceededMemoryWithSkipBytesInMemoryOverheadCheckConfig() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(100)
                                                  .maxSizeInBytes(10)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .skipBytesInMemoryOverheadCheck(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of(eventCount, eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            //Do nothing
          }
        };
      };

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      // Expected 0 since we persisted after the add
      Assert.assertEquals(
          0,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      // Expected 0 since we persisted after the add
      Assert.assertEquals(
          0,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
    }
  }

  @Test
  public void testTaskCleanupInMemoryCounterAfterCloseWithRowInMemory() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(100)
                                                  .maxSizeInBytes(10000)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of(eventCount, eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            //Do nothing
          }
        };
      };

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);

      // Still under maxSizeInBytes after the add. Hence, we do not persist yet
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      int currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      int sinkSizeOverhead = 1 * StreamAppenderator.ROUGH_OVERHEAD_PER_SINK;
      Assert.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // Close with row still in memory (no persist)
      appenderator.close();

      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory());
    }
  }

  @Test
  public void testMaxBytesInMemoryInMultipleSinks() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(100)
                                                  .maxSizeInBytes(31100)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of(eventCount, eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            //Do nothing
          }
        };
      };

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar", 1), committerSupplier);

      // Still under maxSizeInBytes after the add. Hence, we do not persist yet
      //expectedSizeInBytes = 44(map overhead) + 28 (TimeAndDims overhead) + 56 (aggregator metrics) + 54 (dimsKeySize) = 182 + 1 byte when null handling is enabled
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      int currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      int sinkSizeOverhead = 2 * StreamAppenderator.ROUGH_OVERHEAD_PER_SINK;
      // currHydrant in the sink still has > 0 bytesInMemory since we do not persist yet
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      Assert.assertEquals(
          (2 * currentInMemoryIndexSize) + sinkSizeOverhead,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // We do multiple more adds to the same sink to cause persist.
      for (int i = 0; i < 49; i++) {
        appenderator.add(IDENTIFIERS.get(0), ir("2000", "bar_" + i, 1), committerSupplier);
        appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar_" + i, 1), committerSupplier);
      }
      sinkSizeOverhead = 2 * StreamAppenderator.ROUGH_OVERHEAD_PER_SINK;
      // currHydrant size is 0 since we just persist all indexes to disk.
      currentInMemoryIndexSize = 0;
      // We are now over maxSizeInBytes after the add. Hence, we do a persist.
      // currHydrant in the sink has 0 bytesInMemory since we just did a persist
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      // Mapped index size is the memory still needed after we persisted indexes. Note that the segments have
      // 1 dimension columns, 2 metric column, 1 time column.
      int mappedIndexSize = 2 * (1012 + (2 * StreamAppenderator.ROUGH_OVERHEAD_PER_METRIC_COLUMN_HOLDER) +
                                 StreamAppenderator.ROUGH_OVERHEAD_PER_DIMENSION_COLUMN_HOLDER +
                                 StreamAppenderator.ROUGH_OVERHEAD_PER_TIME_COLUMN_HOLDER);
      Assert.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead + mappedIndexSize,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // Add a single row after persisted to sink 0
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "bob", 1), committerSupplier);
      // currHydrant in the sink still has > 0 bytesInMemory since we do not persist yet
      currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assert.assertEquals(
          0,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      Assert.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead + mappedIndexSize,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      // Now add a single row to sink 1
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bob", 1), committerSupplier);
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      Assert.assertEquals(
          (2 * currentInMemoryIndexSize) + sinkSizeOverhead + mappedIndexSize,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // We do multiple more adds to the both sink to cause persist.
      for (int i = 0; i < 34; i++) {
        appenderator.add(IDENTIFIERS.get(0), ir("2000", "bar_" + i, 1), committerSupplier);
        appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar_" + i, 1), committerSupplier);
      }
      // currHydrant size is 0 since we just persist all indexes to disk.
      currentInMemoryIndexSize = 0;
      // We are now over maxSizeInBytes after the add. Hence, we do a persist.
      // currHydrant in the sink has 0 bytesInMemory since we just did a persist
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assert.assertEquals(
          currentInMemoryIndexSize,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      // Mapped index size is the memory still needed after we persisted indexes. Note that the segments have
      // 1 dimension columns, 2 metric column, 1 time column. However, we have two indexes now from the two pervious
      // persists.
      mappedIndexSize = 2 * (2 * (1012 + (2 * StreamAppenderator.ROUGH_OVERHEAD_PER_METRIC_COLUMN_HOLDER) +
                                  StreamAppenderator.ROUGH_OVERHEAD_PER_DIMENSION_COLUMN_HOLDER +
                                  StreamAppenderator.ROUGH_OVERHEAD_PER_TIME_COLUMN_HOLDER));
      Assert.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead + mappedIndexSize,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      appenderator.close();
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory());
    }
  }

  @Test
  public void testIgnoreMaxBytesInMemory() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(100)
                                                  .maxSizeInBytes(-1)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of(eventCount, eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            //Do nothing
          }
        };
      };

      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.startJob();
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      //we still calculate the size even when ignoring it to make persist decision
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      Assert.assertEquals(
          182 + nullHandlingOverhead,
          ((StreamAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assert.assertEquals(1, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar", 1), committerSupplier);
      int sinkSizeOverhead = 2 * StreamAppenderator.ROUGH_OVERHEAD_PER_SINK;
      Assert.assertEquals(
          (364 + 2 * nullHandlingOverhead) + sinkSizeOverhead,
          ((StreamAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      Assert.assertEquals(2, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.close();
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
    }
  }

  @Test
  public void testMaxRowsInMemory() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(3)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = new Supplier<Committer>()
      {
        @Override
        public Committer get()
        {
          final Object metadata = ImmutableMap.of("eventCount", eventCount.get());

          return new Committer()
          {
            @Override
            public Object getMetadata()
            {
              return metadata;
            }

            @Override
            public void run()
            {
              // Do nothing
            }
          };
        }
      };

      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.startJob();
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      Assert.assertEquals(1, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar", 1), committerSupplier);
      Assert.assertEquals(2, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar", 1), committerSupplier);
      Assert.assertEquals(2, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "baz", 1), committerSupplier);
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "qux", 1), committerSupplier);
      Assert.assertEquals(1, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "bob", 1), committerSupplier);
      Assert.assertEquals(2, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.persistAll(committerSupplier.get());
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.close();
    }
  }

  @Test
  public void testMaxRowsInMemoryDisallowIncrementalPersists() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(3)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of("eventCount", eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            // Do nothing
          }
        };
      };

      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.startJob();
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier, false);
      Assert.assertEquals(1, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar", 1), committerSupplier, false);
      Assert.assertEquals(2, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar", 1), committerSupplier, false);
      Assert.assertEquals(2, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "baz", 1), committerSupplier, false);
      Assert.assertEquals(3, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "qux", 1), committerSupplier, false);
      Assert.assertEquals(4, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "bob", 1), committerSupplier, false);
      Assert.assertEquals(5, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.persistAll(committerSupplier.get());
      Assert.assertEquals(0, ((StreamAppenderator) appenderator).getRowsInMemory());
      appenderator.close();
    }
  }

  @Test
  public void testRestoreFromDisk() throws Exception
  {
    final RealtimeTuningConfig tuningConfig;
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(2)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      tuningConfig = tester.getTuningConfig();

      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = new Supplier<Committer>()
      {
        @Override
        public Committer get()
        {
          final Object metadata = ImmutableMap.of("eventCount", eventCount.get());

          return new Committer()
          {
            @Override
            public Object getMetadata()
            {
              return metadata;
            }

            @Override
            public void run()
            {
              // Do nothing
            }
          };
        }
      };

      appenderator.startJob();
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "bar", 2), committerSupplier);
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "baz", 3), committerSupplier);
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "qux", 4), committerSupplier);
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "bob", 5), committerSupplier);
      appenderator.close();

      try (
          final StreamAppenderatorTester tester2 =
              new StreamAppenderatorTester.Builder().maxRowsInMemory(2)
                                                    .basePersistDirectory(tuningConfig.getBasePersistDirectory())
                                                    .enablePushFailure(true)
                                                    .build()) {
        final Appenderator appenderator2 = tester2.getAppenderator();
        Assert.assertEquals(ImmutableMap.of("eventCount", 4), appenderator2.startJob());
        Assert.assertEquals(ImmutableList.of(IDENTIFIERS.get(0)), appenderator2.getSegments());
        Assert.assertEquals(4, appenderator2.getRowCount(IDENTIFIERS.get(0)));
      }
    }
  }

  @Test(timeout = 60_000L)
  public void testTotalRowCount() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(3)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      final ConcurrentMap<String, String> commitMetadata = new ConcurrentHashMap<>();
      final Supplier<Committer> committerSupplier = committerSupplierFromConcurrentMap(commitMetadata);

      Assert.assertEquals(0, appenderator.getTotalRowCount());
      appenderator.startJob();
      Assert.assertEquals(0, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);
      Assert.assertEquals(1, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "bar", 1), committerSupplier);
      Assert.assertEquals(2, appenderator.getTotalRowCount());

      appenderator.persistAll(committerSupplier.get()).get();
      Assert.assertEquals(2, appenderator.getTotalRowCount());
      appenderator.drop(IDENTIFIERS.get(0)).get();
      Assert.assertEquals(1, appenderator.getTotalRowCount());
      appenderator.drop(IDENTIFIERS.get(1)).get();
      Assert.assertEquals(0, appenderator.getTotalRowCount());

      appenderator.add(IDENTIFIERS.get(2), ir("2001", "bar", 1), committerSupplier);
      Assert.assertEquals(1, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(2), ir("2001", "baz", 1), committerSupplier);
      Assert.assertEquals(2, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(2), ir("2001", "qux", 1), committerSupplier);
      Assert.assertEquals(3, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(2), ir("2001", "bob", 1), committerSupplier);
      Assert.assertEquals(4, appenderator.getTotalRowCount());

      appenderator.persistAll(committerSupplier.get()).get();
      Assert.assertEquals(4, appenderator.getTotalRowCount());
      appenderator.drop(IDENTIFIERS.get(2)).get();
      Assert.assertEquals(0, appenderator.getTotalRowCount());

      appenderator.close();
      Assert.assertEquals(0, appenderator.getTotalRowCount());
    }
  }

  @Test
  public void testVerifyRowIngestionMetrics() throws Exception
  {
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(5)
                                                  .maxSizeInBytes(10000L)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .rowIngestionMeters(rowIngestionMeters)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", "invalid_met"), Committers.nilSupplier());
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), Committers.nilSupplier());

      Assert.assertEquals(1, rowIngestionMeters.getProcessed());
      Assert.assertEquals(1, rowIngestionMeters.getProcessedWithError());
      Assert.assertEquals(0, rowIngestionMeters.getUnparseable());
      Assert.assertEquals(0, rowIngestionMeters.getThrownAway());
    }
  }

  @Test
  public void testDelayedDrop() throws Exception
  {
    class TestScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor
    {
      ScheduledFuture<?> scheduledFuture;

      public TestScheduledThreadPoolExecutor()
      {
        super(1);
      }

      @Override
      public ScheduledFuture<?> schedule(
          Runnable command,
          long delay, TimeUnit unit
      )
      {
        ScheduledFuture<?> future = super.schedule(command, delay, unit);
        scheduledFuture = future;
        return future;
      }

      ScheduledFuture<?> getLastScheduledFuture()
      {
        return scheduledFuture;
      }
    }

    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(2)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .withSegmentDropDelayInMilli(1000)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();
      TestScheduledThreadPoolExecutor testExec = new TestScheduledThreadPoolExecutor();
      ((StreamAppenderator) appenderator).setExec(testExec);

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 2), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "foo", 4), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001T03", "foo", 64), Suppliers.ofInstance(Committers.nil()));

      // Query1: 2000/2001
      final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(StreamAppenderatorTester.DATASOURCE)
                                           .intervals(ImmutableList.of(Intervals.of("2000/2001")))
                                           .aggregators(
                                               Arrays.asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      appenderator.drop(IDENTIFIERS.get(0)).get();

      // segment 0 won't be dropped immediately
      final List<Result<TimeseriesResultValue>> results1 =
          QueryPlus.wrap(query1).run(appenderator, ResponseContext.createEmpty()).toList();
      Assert.assertEquals(
          "query1",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 3L, "met", 7L))
              )
          ),
          results1
      );

      // segment 0 would eventually be dropped at some time after 1 secs drop delay
      testExec.getLastScheduledFuture().get(5000, TimeUnit.MILLISECONDS);

      final List<Result<TimeseriesResultValue>> results = QueryPlus.wrap(query1)
                                                                   .run(appenderator, ResponseContext.createEmpty())
                                                                   .toList();
      List<Result<TimeseriesResultValue>> expectedResults =
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 1L, "met", 4L))
              )
          );
      Assert.assertEquals("query after dropped", expectedResults, results);
    }
  }

  @Test
  public void testQueryByIntervals() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(2)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 2), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "foo", 4), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001T03", "foo", 64), Suppliers.ofInstance(Committers.nil()));

      // Query1: 2000/2001
      final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(StreamAppenderatorTester.DATASOURCE)
                                           .intervals(ImmutableList.of(Intervals.of("2000/2001")))
                                           .aggregators(
                                               Arrays.asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      final List<Result<TimeseriesResultValue>> results1 =
          QueryPlus.wrap(query1).run(appenderator, ResponseContext.createEmpty()).toList();
      Assert.assertEquals(
          "query1",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 3L, "met", 7L))
              )
          ),
          results1
      );

      // Query2: 2000/2002
      final TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(StreamAppenderatorTester.DATASOURCE)
                                           .intervals(ImmutableList.of(Intervals.of("2000/2002")))
                                           .aggregators(
                                               Arrays.asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      final List<Result<TimeseriesResultValue>> results2 =
          QueryPlus.wrap(query2).run(appenderator, ResponseContext.createEmpty()).toList();
      Assert.assertEquals(
          "query2",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 3L, "met", 7L))
              ),
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 4L, "met", 120L))
              )
          ),
          results2
      );

      // Query3: 2000/2001T01
      final TimeseriesQuery query3 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(StreamAppenderatorTester.DATASOURCE)
                                           .intervals(ImmutableList.of(Intervals.of("2000/2001T01")))
                                           .aggregators(
                                               Arrays.asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      final List<Result<TimeseriesResultValue>> results3 =
          QueryPlus.wrap(query3).run(appenderator, ResponseContext.createEmpty()).toList();
      Assert.assertEquals(
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 3L, "met", 7L))
              ),
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 1L, "met", 8L))
              )
          ),
          results3
      );

      // Query4: 2000/2001T01, 2001T03/2001T04
      final TimeseriesQuery query4 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(StreamAppenderatorTester.DATASOURCE)
                                           .intervals(
                                               ImmutableList.of(
                                                   Intervals.of("2000/2001T01"),
                                                   Intervals.of("2001T03/2001T04")
                                               )
                                           )
                                           .aggregators(
                                               Arrays.asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      final List<Result<TimeseriesResultValue>> results4 =
          QueryPlus.wrap(query4).run(appenderator, ResponseContext.createEmpty()).toList();
      Assert.assertEquals(
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 3L, "met", 7L))
              ),
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 2L, "met", 72L))
              )
          ),
          results4
      );
    }
  }

  @Test
  public void testQueryBySegments() throws Exception
  {
    try (
        final StreamAppenderatorTester tester =
            new StreamAppenderatorTester.Builder().maxRowsInMemory(2)
                                                  .basePersistDirectory(temporaryFolder.newFolder())
                                                  .enablePushFailure(true)
                                                  .build()) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 2), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "foo", 4), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), ir("2001T03", "foo", 64), Suppliers.ofInstance(Committers.nil()));

      // Query1: segment #2
      final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(StreamAppenderatorTester.DATASOURCE)
                                           .aggregators(
                                               Arrays.asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .intervals(
                                               new MultipleSpecificSegmentSpec(
                                                   ImmutableList.of(
                                                       new SegmentDescriptor(
                                                           IDENTIFIERS.get(2).getInterval(),
                                                           IDENTIFIERS.get(2).getVersion(),
                                                           IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                       )
                                                   )
                                               )
                                           )
                                           .build();

      final List<Result<TimeseriesResultValue>> results1 =
          QueryPlus.wrap(query1).run(appenderator, ResponseContext.createEmpty()).toList();
      Assert.assertEquals(
          "query1",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 4L, "met", 120L))
              )
          ),
          results1
      );

      // Query2: segment #2, partial
      final TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(StreamAppenderatorTester.DATASOURCE)
                                           .aggregators(
                                               Arrays.asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .intervals(
                                               new MultipleSpecificSegmentSpec(
                                                   ImmutableList.of(
                                                       new SegmentDescriptor(
                                                           Intervals.of("2001/PT1H"),
                                                           IDENTIFIERS.get(2).getVersion(),
                                                           IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                       )
                                                   )
                                               )
                                           )
                                           .build();

      final List<Result<TimeseriesResultValue>> results2 =
          QueryPlus.wrap(query2).run(appenderator, ResponseContext.createEmpty()).toList();
      Assert.assertEquals(
          "query2",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 1L, "met", 8L))
              )
          ),
          results2
      );

      // Query3: segment #2, two disjoint intervals
      final TimeseriesQuery query3 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(StreamAppenderatorTester.DATASOURCE)
                                           .aggregators(
                                               Arrays.asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .intervals(
                                               new MultipleSpecificSegmentSpec(
                                                   ImmutableList.of(
                                                       new SegmentDescriptor(
                                                           Intervals.of("2001/PT1H"),
                                                           IDENTIFIERS.get(2).getVersion(),
                                                           IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                       ),
                                                       new SegmentDescriptor(
                                                           Intervals.of("2001T03/PT1H"),
                                                           IDENTIFIERS.get(2).getVersion(),
                                                           IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                       )
                                                   )
                                               )
                                           )
                                           .build();

      final List<Result<TimeseriesResultValue>> results3 =
          QueryPlus.wrap(query3).run(appenderator, ResponseContext.createEmpty()).toList();
      Assert.assertEquals(
          "query3",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.of("count", 2L, "met", 72L))
              )
          ),
          results3
      );

      final ScanQuery query4 = Druids.newScanQueryBuilder()
                                     .dataSource(StreamAppenderatorTester.DATASOURCE)
                                     .intervals(
                                         new MultipleSpecificSegmentSpec(
                                             ImmutableList.of(
                                                 new SegmentDescriptor(
                                                     Intervals.of("2001/PT1H"),
                                                     IDENTIFIERS.get(2).getVersion(),
                                                     IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                 ),
                                                 new SegmentDescriptor(
                                                     Intervals.of("2001T03/PT1H"),
                                                     IDENTIFIERS.get(2).getVersion(),
                                                     IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                 )
                                             )
                                         )
                                     )
                                     .order(ScanQuery.Order.ASCENDING)
                                     .batchSize(10)
                                     .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                     .build();
      final List<ScanResultValue> results4 =
          QueryPlus.wrap(query4).run(appenderator, ResponseContext.createEmpty()).toList();
      Assert.assertEquals(2, results4.size()); // 2 segments, 1 row per segment
      Assert.assertArrayEquals(new String[]{"__time", "dim", "count", "met"}, results4.get(0).getColumns().toArray());
      Assert.assertArrayEquals(
          new Object[]{DateTimes.of("2001").getMillis(), "foo", 1L, 8L},
          ((List<Object>) ((List<Object>) results4.get(0).getEvents()).get(0)).toArray()
      );
      Assert.assertArrayEquals(new String[]{"__time", "dim", "count", "met"}, results4.get(0).getColumns().toArray());
      Assert.assertArrayEquals(
          new Object[]{DateTimes.of("2001T03").getMillis(), "foo", 1L, 64L},
          ((List<Object>) ((List<Object>) results4.get(1).getEvents()).get(0)).toArray()
      );
    }
  }

  @Test
  public void testSchemaAnnouncement() throws Exception
  {
    TestSchemaAnnouncer dataSegmentAnnouncer = new TestSchemaAnnouncer();

    try (final StreamAppenderatorTester tester =
             new StreamAppenderatorTester.Builder().maxRowsInMemory(2)
                                                   .enablePushFailure(true)
                                                   .basePersistDirectory(temporaryFolder.newFolder())
                                                   .build(dataSegmentAnnouncer, CentralizedDatasourceSchemaConfig.create())) {
      final StreamAppenderator appenderator = (StreamAppenderator) tester.getAppenderator();

      final ConcurrentMap<String, String> commitMetadata = new ConcurrentHashMap<>();
      final Supplier<Committer> committerSupplier = committerSupplierFromConcurrentMap(commitMetadata);
      StreamAppenderator.SinkSchemaAnnouncer sinkSchemaAnnouncer = appenderator.getSinkSchemaAnnouncer();

      // startJob
      Assert.assertEquals(null, appenderator.startJob());

      // getDataSource
      Assert.assertEquals(StreamAppenderatorTester.DATASOURCE, appenderator.getDataSource());

      // add first row
      commitMetadata.put("x", "1");
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), committerSupplier);

      // trigger schema computation
      sinkSchemaAnnouncer.computeAndAnnounce();

      // verify schema
      List<Pair<String, SegmentSchemas>> announcedAbsoluteSchema = dataSegmentAnnouncer.getAnnouncedAbsoluteSchema();
      List<Pair<String, SegmentSchemas>> announcedDeltaSchema = dataSegmentAnnouncer.getAnnouncedDeltaSchema();

      Assert.assertEquals(1, announcedAbsoluteSchema.size());
      Assert.assertEquals(1, announcedDeltaSchema.size());

      // verify absolute schema
      Assert.assertEquals(appenderator.getId(), announcedAbsoluteSchema.get(0).lhs);
      List<SegmentSchemas.SegmentSchema> segmentSchemas = announcedAbsoluteSchema.get(0).rhs.getSegmentSchemaList();
      Assert.assertEquals(1, segmentSchemas.size());
      SegmentSchemas.SegmentSchema absoluteSchemaId1Row1 = segmentSchemas.get(0);
      Assert.assertEquals(IDENTIFIERS.get(0).asSegmentId().toString(), absoluteSchemaId1Row1.getSegmentId());
      Assert.assertEquals(1, absoluteSchemaId1Row1.getNumRows().intValue());
      Assert.assertFalse(absoluteSchemaId1Row1.isDelta());
      Assert.assertEquals(Collections.emptyList(), absoluteSchemaId1Row1.getUpdatedColumns());
      Assert.assertEquals(Lists.newArrayList("__time", "dim", "count", "met"), absoluteSchemaId1Row1.getNewColumns());
      Assert.assertEquals(
          ImmutableMap.of("__time", ColumnType.LONG, "count", ColumnType.LONG, "dim", ColumnType.STRING, "met", ColumnType.LONG),
          absoluteSchemaId1Row1.getColumnTypeMap());

      // verify delta schema
      Assert.assertEquals(appenderator.getId(), announcedDeltaSchema.get(0).lhs);
      segmentSchemas = announcedDeltaSchema.get(0).rhs.getSegmentSchemaList();
      SegmentSchemas.SegmentSchema deltaSchemaId1Row1 = segmentSchemas.get(0);
      Assert.assertEquals(1, segmentSchemas.size());
      Assert.assertEquals(IDENTIFIERS.get(0).asSegmentId().toString(), deltaSchemaId1Row1.getSegmentId());
      Assert.assertEquals(1, deltaSchemaId1Row1.getNumRows().intValue());
      // absolute schema is sent for a new sink
      Assert.assertFalse(deltaSchemaId1Row1.isDelta());
      Assert.assertEquals(Collections.emptyList(), deltaSchemaId1Row1.getUpdatedColumns());
      Assert.assertEquals(Lists.newArrayList("__time", "dim", "count", "met"), deltaSchemaId1Row1.getNewColumns());
      Assert.assertEquals(
          ImmutableMap.of("__time", ColumnType.LONG, "count", ColumnType.LONG, "dim", ColumnType.STRING, "met", ColumnType.LONG),
          deltaSchemaId1Row1.getColumnTypeMap());

      dataSegmentAnnouncer.clear();

      // add second row
      commitMetadata.put("x", "2");
      appenderator.add(IDENTIFIERS.get(0), ir("2000", "bar", 2), committerSupplier);

      // trigger schema computation
      sinkSchemaAnnouncer.computeAndAnnounce();

      // verify schema
      announcedAbsoluteSchema = dataSegmentAnnouncer.getAnnouncedAbsoluteSchema();
      announcedDeltaSchema = dataSegmentAnnouncer.getAnnouncedDeltaSchema();

      Assert.assertEquals(1, announcedAbsoluteSchema.size());
      Assert.assertEquals(1, announcedDeltaSchema.size());

      // verify absolute schema
      Assert.assertEquals(appenderator.getId(), announcedAbsoluteSchema.get(0).lhs);
      segmentSchemas = announcedAbsoluteSchema.get(0).rhs.getSegmentSchemaList();
      Assert.assertEquals(1, segmentSchemas.size());
      SegmentSchemas.SegmentSchema absoluteSchemaId1Row2 = segmentSchemas.get(0);
      Assert.assertEquals(IDENTIFIERS.get(0).asSegmentId().toString(), absoluteSchemaId1Row2.getSegmentId());
      Assert.assertEquals(2, absoluteSchemaId1Row2.getNumRows().intValue());
      Assert.assertFalse(absoluteSchemaId1Row2.isDelta());
      Assert.assertEquals(Collections.emptyList(), absoluteSchemaId1Row2.getUpdatedColumns());
      Assert.assertEquals(Lists.newArrayList("__time", "dim", "count", "met"), absoluteSchemaId1Row2.getNewColumns());
      Assert.assertEquals(
          ImmutableMap.of("__time", ColumnType.LONG, "count", ColumnType.LONG, "dim", ColumnType.STRING, "met", ColumnType.LONG),
          absoluteSchemaId1Row2.getColumnTypeMap());

      // verify delta
      Assert.assertEquals(appenderator.getId(), announcedDeltaSchema.get(0).lhs);
      segmentSchemas = announcedDeltaSchema.get(0).rhs.getSegmentSchemaList();
      SegmentSchemas.SegmentSchema deltaSchemaId1Row2 = segmentSchemas.get(0);
      Assert.assertEquals(1, segmentSchemas.size());
      Assert.assertEquals(IDENTIFIERS.get(0).asSegmentId().toString(), deltaSchemaId1Row2.getSegmentId());
      Assert.assertEquals(2, deltaSchemaId1Row2.getNumRows().intValue());
      Assert.assertTrue(deltaSchemaId1Row2.isDelta());
      Assert.assertEquals(Collections.emptyList(), deltaSchemaId1Row2.getUpdatedColumns());
      Assert.assertEquals(Collections.emptyList(), deltaSchemaId1Row2.getNewColumns());
      Assert.assertEquals(Collections.emptyMap(), deltaSchemaId1Row2.getColumnTypeMap());

      dataSegmentAnnouncer.clear();

      // add first row for second segment
      commitMetadata.put("x", "3");
      appenderator.add(IDENTIFIERS.get(1), ir("2000", "qux", 4), committerSupplier);

      sinkSchemaAnnouncer.computeAndAnnounce();

      // verify schema
      announcedAbsoluteSchema = dataSegmentAnnouncer.getAnnouncedAbsoluteSchema();
      announcedDeltaSchema = dataSegmentAnnouncer.getAnnouncedDeltaSchema();

      Assert.assertEquals(1, announcedAbsoluteSchema.size());
      Assert.assertEquals(1, announcedDeltaSchema.size());

      // verify absolute schema
      Assert.assertEquals(appenderator.getId(), announcedAbsoluteSchema.get(0).lhs);
      segmentSchemas = announcedAbsoluteSchema.get(0).rhs.getSegmentSchemaList();
      Assert.assertEquals(2, segmentSchemas.size());
      SegmentSchemas.SegmentSchema absoluteSchemaId2Row1 =
          segmentSchemas.stream()
                        .filter(v -> v.getSegmentId().equals(IDENTIFIERS.get(1).asSegmentId().toString()))
                        .findFirst()
                        .get();
      Assert.assertEquals(IDENTIFIERS.get(1).asSegmentId().toString(), absoluteSchemaId2Row1.getSegmentId());
      Assert.assertEquals(1, absoluteSchemaId2Row1.getNumRows().intValue());
      Assert.assertFalse(absoluteSchemaId2Row1.isDelta());
      Assert.assertEquals(Collections.emptyList(), absoluteSchemaId2Row1.getUpdatedColumns());
      Assert.assertEquals(Lists.newArrayList("__time", "dim", "count", "met"), absoluteSchemaId2Row1.getNewColumns());
      Assert.assertEquals(
          ImmutableMap.of("__time", ColumnType.LONG, "count", ColumnType.LONG, "dim", ColumnType.STRING, "met", ColumnType.LONG),
          absoluteSchemaId2Row1.getColumnTypeMap());

      // verify delta
      Assert.assertEquals(appenderator.getId(), announcedDeltaSchema.get(0).lhs);
      segmentSchemas = announcedDeltaSchema.get(0).rhs.getSegmentSchemaList();
      SegmentSchemas.SegmentSchema deltaSchemaId2Row1 =
          segmentSchemas.stream()
                        .filter(v -> v.getSegmentId().equals(IDENTIFIERS.get(1).asSegmentId().toString()))
                        .findFirst()
                        .get();
      Assert.assertEquals(1, segmentSchemas.size());
      Assert.assertEquals(IDENTIFIERS.get(1).asSegmentId().toString(), deltaSchemaId2Row1.getSegmentId());
      Assert.assertEquals(1, deltaSchemaId2Row1.getNumRows().intValue());
      Assert.assertFalse(deltaSchemaId2Row1.isDelta());
      Assert.assertEquals(Collections.emptyList(), deltaSchemaId2Row1.getUpdatedColumns());
      Assert.assertEquals(Lists.newArrayList("__time", "dim", "count", "met"), deltaSchemaId2Row1.getNewColumns());
      Assert.assertEquals(
          ImmutableMap.of("__time", ColumnType.LONG, "count", ColumnType.LONG, "dim", ColumnType.STRING, "met", ColumnType.LONG),
          deltaSchemaId2Row1.getColumnTypeMap());
    }
  }

  private static SegmentIdWithShardSpec si(String interval, String version, int partitionNum)
  {
    return new SegmentIdWithShardSpec(
        StreamAppenderatorTester.DATASOURCE,
        Intervals.of(interval),
        version,
        new LinearShardSpec(partitionNum)
    );
  }

  static InputRow ir(String ts, String dim, Object met)
  {
    return new MapBasedInputRow(
        DateTimes.of(ts).getMillis(),
        ImmutableList.of("dim"),
        ImmutableMap.of(
            "dim",
            dim,
            "met",
            met
        )
    );
  }

  private static Supplier<Committer> committerSupplierFromConcurrentMap(final ConcurrentMap<String, String> map)
  {
    return new Supplier<Committer>()
    {
      @Override
      public Committer get()
      {
        final Map<String, String> mapCopy = ImmutableMap.copyOf(map);

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return mapCopy;
          }

          @Override
          public void run()
          {
            // Do nothing
          }
        };
      }
    };
  }

  private static <T> List<T> sorted(final List<T> xs)
  {
    final List<T> xsSorted = Lists.newArrayList(xs);
    Collections.sort(
        xsSorted,
        (T a, T b) -> {
          if (a instanceof SegmentIdWithShardSpec && b instanceof SegmentIdWithShardSpec) {
            return ((SegmentIdWithShardSpec) a).compareTo(((SegmentIdWithShardSpec) b));
          } else if (a instanceof DataSegment && b instanceof DataSegment) {
            return ((DataSegment) a).getId().compareTo(((DataSegment) b).getId());
          } else {
            throw new IllegalStateException("BAD");
          }
        }
    );
    return xsSorted;
  }

  static class TestSchemaAnnouncer implements DataSegmentAnnouncer
  {
    private List<Pair<String, SegmentSchemas>> announcedAbsoluteSchema = new ArrayList<>();
    private List<Pair<String, SegmentSchemas>> announcedDeltaSchema = new ArrayList<>();
    private List<String> unnanouncementEvents = new ArrayList<>();

    @Override
    public void announceSegment(DataSegment segment)
    {
      // noop
    }

    @Override
    public void unannounceSegment(DataSegment segment)
    {
      // noop
    }

    @Override
    public void announceSegments(Iterable<DataSegment> segments)
    {
      // noop
    }

    @Override
    public void unannounceSegments(Iterable<DataSegment> segments)
    {
      // noop
    }

    @Override
    public void announceSegmentSchemas(
        String taskId,
        SegmentSchemas segmentSchemas,
        @Nullable SegmentSchemas segmentSchemasChange
    )
    {
      announcedAbsoluteSchema.add(Pair.of(taskId, segmentSchemas));
      announcedDeltaSchema.add(Pair.of(taskId, segmentSchemasChange));
    }

    @Override
    public void removeSegmentSchemasForTask(String taskId)
    {
      unnanouncementEvents.add(taskId);
    }

    public List<Pair<String, SegmentSchemas>> getAnnouncedAbsoluteSchema()
    {
      return announcedAbsoluteSchema;
    }

    public List<Pair<String, SegmentSchemas>> getAnnouncedDeltaSchema()
    {
      return announcedDeltaSchema;
    }

    public List<String> getUnnanouncementEvents()
    {
      return unnanouncementEvents;
    }

    public void clear()
    {
      announcedAbsoluteSchema.clear();
      announcedDeltaSchema.clear();
      unnanouncementEvents.clear();
    }
  }
}
