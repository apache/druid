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

package io.druid.segment.realtime.appenderator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.realtime.appenderator.BaseAppenderatorDriver.SegmentsForSequence;
import io.druid.segment.realtime.appenderator.StreamAppenderatorDriverTest.TestSegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentWithState.SegmentState;
import io.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BatchAppenderatorDriverTest extends EasyMockSupport
{
  private static final String DATA_SOURCE = "foo";
  private static final String VERSION = "abc123";
  private static final int MAX_ROWS_IN_MEMORY = 100;
  private static final long TIMEOUT = 1000;

  private static final List<InputRow> ROWS = Arrays.<InputRow>asList(
      new MapBasedInputRow(
          DateTimes.of("2000"),
          ImmutableList.of("dim1"),
          ImmutableMap.<String, Object>of("dim1", "foo", "met1", "1")
      ),
      new MapBasedInputRow(
          DateTimes.of("2000T01"),
          ImmutableList.of("dim1"),
          ImmutableMap.<String, Object>of("dim1", "foo", "met1", 2.0)
      ),
      new MapBasedInputRow(
          DateTimes.of("2000T01"),
          ImmutableList.of("dim2"),
          ImmutableMap.<String, Object>of("dim2", "bar", "met1", 2.0)
      )
  );

  private SegmentAllocator allocator;
  private AppenderatorTester appenderatorTester;
  private BatchAppenderatorDriver driver;
  private DataSegmentKiller dataSegmentKiller;

  @Before
  public void setup()
  {
    appenderatorTester = new AppenderatorTester(MAX_ROWS_IN_MEMORY);
    allocator = new TestSegmentAllocator(DATA_SOURCE, Granularities.HOUR);
    dataSegmentKiller = createStrictMock(DataSegmentKiller.class);
    driver = new BatchAppenderatorDriver(
        appenderatorTester.getAppenderator(),
        allocator,
        new TestUsedSegmentChecker(appenderatorTester),
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
    Assert.assertNull(driver.startJob());

    for (InputRow row : ROWS) {
      Assert.assertTrue(driver.add(row, "dummy").isOk());
    }

    checkSegmentStates(2, SegmentState.APPENDING);

    driver.pushAllAndClear(TIMEOUT);

    checkSegmentStates(2, SegmentState.PUSHED_AND_DROPPED);

    final SegmentsAndMetadata published = driver.publishAll(makeOkPublisher()).get(
        TIMEOUT,
        TimeUnit.MILLISECONDS
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new SegmentIdentifier(DATA_SOURCE, Intervals.of("2000/PT1H"), VERSION, new NumberedShardSpec(0, 0)),
            new SegmentIdentifier(DATA_SOURCE, Intervals.of("2000T01/PT1H"), VERSION, new NumberedShardSpec(0, 0))
        ),
        published.getSegments()
                 .stream()
                 .map(SegmentIdentifier::fromDataSegment)
                 .collect(Collectors.toSet())
    );

    Assert.assertNull(published.getCommitMetadata());
  }

  @Test
  public void testIncrementalPush() throws Exception
  {
    Assert.assertNull(driver.startJob());

    int i = 0;
    for (InputRow row : ROWS) {
      Assert.assertTrue(driver.add(row, "dummy").isOk());

      checkSegmentStates(1, SegmentState.APPENDING);
      checkSegmentStates(i, SegmentState.PUSHED_AND_DROPPED);

      driver.pushAllAndClear(TIMEOUT);
      checkSegmentStates(0, SegmentState.APPENDING);
      checkSegmentStates(++i, SegmentState.PUSHED_AND_DROPPED);
    }

    final SegmentsAndMetadata published = driver.publishAll(makeOkPublisher()).get(
        TIMEOUT,
        TimeUnit.MILLISECONDS
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new SegmentIdentifier(DATA_SOURCE, Intervals.of("2000/PT1H"), VERSION, new NumberedShardSpec(0, 0)),
            new SegmentIdentifier(DATA_SOURCE, Intervals.of("2000T01/PT1H"), VERSION, new NumberedShardSpec(0, 0)),
            new SegmentIdentifier(DATA_SOURCE, Intervals.of("2000T01/PT1H"), VERSION, new NumberedShardSpec(1, 0))
        ),
        published.getSegments()
                 .stream()
                 .map(SegmentIdentifier::fromDataSegment)
                 .collect(Collectors.toSet())
    );

    Assert.assertNull(published.getCommitMetadata());
  }

  @Test
  public void testRestart()
  {
    Assert.assertNull(driver.startJob());
    driver.close();
    appenderatorTester.getAppenderator().close();

    Assert.assertNull(driver.startJob());
  }

  private void checkSegmentStates(int expectedNumSegmentsInState, SegmentState expectedState)
  {
    final SegmentsForSequence segmentsForSequence = driver.getSegments().get("dummy");
    Assert.assertNotNull(segmentsForSequence);
    final List<SegmentWithState> segmentWithStates = segmentsForSequence
        .segmentStateStream()
        .filter(segmentWithState -> segmentWithState.getState() == expectedState)
        .collect(Collectors.toList());

    Assert.assertEquals(expectedNumSegmentsInState, segmentWithStates.size());
  }

  static TransactionalSegmentPublisher makeOkPublisher()
  {
    return (segments, commitMetadata) -> true;
  }
}
