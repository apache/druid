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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.realtime.appenderator.BaseAppenderatorDriver.SegmentsForSequence;
import org.apache.druid.segment.realtime.appenderator.SegmentWithState.SegmentState;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClosedSegmentsSinksBatchAppenderatorDriverTest extends EasyMockSupport
{
  private static final String DATA_SOURCE = "foo";
  private static final String VERSION = "abc123";
  private static final int MAX_ROWS_IN_MEMORY = 100;
  private static final long TIMEOUT = 1000;

  private static final List<InputRow> ROWS = Arrays.asList(
      new MapBasedInputRow(
          DateTimes.of("2000"),
          ImmutableList.of("dim1"),
          ImmutableMap.of("dim1", "foo", "met1", "1")
      ),
      new MapBasedInputRow(
          DateTimes.of("2000T01"),
          ImmutableList.of("dim1"),
          ImmutableMap.of("dim1", "foo", "met1", 2.0)
      ),
      new MapBasedInputRow(
          DateTimes.of("2000T01"),
          ImmutableList.of("dim2"),
          ImmutableMap.of("dim2", "bar", "met1", 2.0)
      )
  );

  private SegmentAllocator allocator;
  private ClosedSegmensSinksBatchAppenderatorTester appenderatorTester;
  private BatchAppenderatorDriver driver;
  private DataSegmentKiller dataSegmentKiller;

  static {
    NullHandling.initializeForTests();
  }

  @Before
  public void setup()
  {
    appenderatorTester = new ClosedSegmensSinksBatchAppenderatorTester(MAX_ROWS_IN_MEMORY);
    allocator = new TestSegmentAllocator(DATA_SOURCE, Granularities.HOUR);
    dataSegmentKiller = createStrictMock(DataSegmentKiller.class);
    driver = new BatchAppenderatorDriver(
        appenderatorTester.getAppenderator(),
        allocator,
        new TestUsedSegmentChecker(appenderatorTester.getPushedSegments()),
        dataSegmentKiller
    );

    EasyMock.replay(dataSegmentKiller);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(dataSegmentKiller);

    driver.clear();
    driver.close();
  }

  @Test
  public void testSimple() throws Exception
  {
    Assert.assertNull(driver.startJob(null));

    for (InputRow row : ROWS) {
      Assert.assertTrue(driver.add(row, "dummy").isOk());
    }

    checkSegmentStates(2, SegmentState.APPENDING);

    driver.pushAllAndClear(TIMEOUT);

    checkSegmentStates(2, SegmentState.PUSHED_AND_DROPPED);

    final SegmentsAndCommitMetadata published =
        driver.publishAll(null, null, Collections.emptySet(), makeOkPublisher(), Function.identity())
              .get(TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertEquals(
        ImmutableSet.of(
            new SegmentIdWithShardSpec(DATA_SOURCE, Intervals.of("2000/PT1H"), VERSION, new NumberedShardSpec(0, 0)),
            new SegmentIdWithShardSpec(DATA_SOURCE, Intervals.of("2000T01/PT1H"), VERSION, new NumberedShardSpec(0, 0))
        ),
        published.getSegments()
                 .stream()
                 .map(SegmentIdWithShardSpec::fromDataSegment)
                 .collect(Collectors.toSet())
    );

    Assert.assertNull(published.getCommitMetadata());
  }

  @Test(timeout = 5000L)
  public void testIncrementalPush() throws Exception
  {
    Assert.assertNull(driver.startJob(null));

    int i = 0;
    for (InputRow row : ROWS) {
      Assert.assertTrue(driver.add(row, "dummy").isOk());

      checkSegmentStates(1, SegmentState.APPENDING);
      checkSegmentStates(i, SegmentState.PUSHED_AND_DROPPED);

      driver.pushAllAndClear(TIMEOUT);
      checkSegmentStates(0, SegmentState.APPENDING);
      checkSegmentStates(++i, SegmentState.PUSHED_AND_DROPPED);
    }

    final SegmentsAndCommitMetadata published =
        driver.publishAll(null, null, Collections.emptySet(), makeOkPublisher(), Function.identity())
              .get(TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertEquals(
        ImmutableSet.of(
            new SegmentIdWithShardSpec(DATA_SOURCE, Intervals.of("2000/PT1H"), VERSION, new NumberedShardSpec(0, 0)),
            new SegmentIdWithShardSpec(DATA_SOURCE, Intervals.of("2000T01/PT1H"), VERSION, new NumberedShardSpec(0, 0)),
            new SegmentIdWithShardSpec(DATA_SOURCE, Intervals.of("2000T01/PT1H"), VERSION, new NumberedShardSpec(1, 0))
        ),
        published.getSegments()
                 .stream()
                 .map(SegmentIdWithShardSpec::fromDataSegment)
                 .collect(Collectors.toSet())
    );

    Assert.assertNull(published.getCommitMetadata());
  }

  @Test
  public void testRestart()
  {
    Assert.assertNull(driver.startJob(null));
    driver.close();
    appenderatorTester.getAppenderator().close();

    Assert.assertNull(driver.startJob(null));
  }

  private void checkSegmentStates(int expectedNumSegmentsInState, SegmentState expectedState)
  {
    final SegmentsForSequence segmentsForSequence = driver.getSegments().get("dummy");
    Assert.assertNotNull(segmentsForSequence);
    final List<SegmentWithState> segmentWithStates = segmentsForSequence
        .allSegmentStateStream()
        .filter(segmentWithState -> segmentWithState.getState() == expectedState)
        .collect(Collectors.toList());

    Assert.assertEquals(expectedNumSegmentsInState, segmentWithStates.size());
  }

  static TransactionalSegmentPublisher makeOkPublisher()
  {
    return (segmentsToBeOverwritten, segmentsToBeDropped, segmentsToPublish, commitMetadata) -> SegmentPublishResult.ok(ImmutableSet.of());
  }

  static class TestSegmentAllocator implements SegmentAllocator
  {
    private final String dataSource;
    private final Granularity granularity;
    private final Map<Long, AtomicInteger> counters = new HashMap<>();

    public TestSegmentAllocator(String dataSource, Granularity granularity)
    {
      this.dataSource = dataSource;
      this.granularity = granularity;
    }

    @Override
    public SegmentIdWithShardSpec allocate(
        final InputRow row,
        final String sequenceName,
        final String previousSegmentId,
        final boolean skipSegmentLineageCheck
    )
    {
      DateTime dateTimeTruncated = granularity.bucketStart(row.getTimestamp());
      final long timestampTruncated = dateTimeTruncated.getMillis();
      counters.putIfAbsent(timestampTruncated, new AtomicInteger());
      final int partitionNum = counters.get(timestampTruncated).getAndIncrement();
      return new SegmentIdWithShardSpec(
          dataSource,
          granularity.bucket(dateTimeTruncated),
          VERSION,
          new NumberedShardSpec(partitionNum, 0)
      );
    }
  }

}
