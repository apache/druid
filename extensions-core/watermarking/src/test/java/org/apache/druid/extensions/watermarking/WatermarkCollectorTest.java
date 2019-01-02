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

package org.apache.druid.extensions.watermarking;

import com.google.common.collect.ImmutableList;
import org.apache.druid.client.DruidServer;
import org.apache.druid.extensions.watermarking.watermarks.BatchCompletenessLowWatermark;
import org.apache.druid.extensions.watermarking.watermarks.DataCompletenessLowWatermark;
import org.apache.druid.extensions.watermarking.watermarks.StableDataHighWatermark;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class WatermarkCollectorTest extends TimelineMetadataCollectorTestBase
{
  // view created first, before any segments are loaded
  @Test
  public void testNoSegmentsAtStartStabilizesCorrectly() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);

    DateTime expected = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expected);

    setupViews();
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));

    segmentAddedLatch = new CountDownLatch(1);
    addSegments(initSegment);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    segmentAddedLatch = new CountDownLatch(initBatchSegments.size());
    addSegments(initBatchSegments);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  // view created after segments are announced
  @Test
  public void testStartupIsCorrect() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(initBatchSegments.size());
    segmentRemovedLatch = new CountDownLatch(0);

    addSegments(initBatchSegments);

    DateTime expected = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expected);

    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  // view created after segments are announced, gaps exist
  @Test
  public void testStartupWithCompleteGapsIsCorrect() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(initBatchSegments.size());
    segmentRemovedLatch = new CountDownLatch(0);

    addSegments(initBatchSegmentsWithGap);

    DateTime expected = DateTimes.of("2011-04-09T00:00:00.000Z");
    DateTime expectedComplete = DateTimes.of("2011-04-06T00:00:00.000Z");
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expectedComplete);
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expectedComplete);

    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedComplete,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedComplete,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  @Test
  public void testStartupWithOverridenBatchCompleteGapIsCorrect() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    addSegments(initSegment);

    setupViews();
    DateTime override = DateTimes.of("2011-04-07T00:00:00.000Z");
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, override);
    cache.set(testDatasource, BatchCompletenessLowWatermark.TYPE, override);
    timelineStore.update(testDatasource, BatchCompletenessLowWatermark.TYPE, override);

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    segmentAddedLatch = new CountDownLatch(initBatchSegments.size());
    addSegments(initBatchSegmentsWithGap, 1);

    DateTime expected = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expected);

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
  }

  @Test
  public void testStartupWithOverridenCompleteGapIsCorrect() throws Exception
  {
    addSegments(initSegment);
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    segmentAddedLatch = new CountDownLatch(initBatchSegmentsWithGap.size());
    DateTime override = DateTimes.of("2011-04-07T00:00:00.000Z");
    addConditionLatch(DataCompletenessLowWatermark.TYPE, override);
    cache.set(testDatasource, DataCompletenessLowWatermark.TYPE, override);
    timelineStore.update(testDatasource, DataCompletenessLowWatermark.TYPE, override);
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));

    DateTime expected = DateTimes.of("2011-04-09T00:00:00.000Z");
    DateTime expectedBatch = DateTimes.of("2011-04-06T00:00:00.000Z");
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(StableDataHighWatermark.TYPE, expected);

    addSegments(initBatchSegmentsWithGap, 1);

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedBatch,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  @Test
  public void testRealtimeSegmentInitialization() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    DateTime expected = DateTimes.of("2011-04-13T00:00:00.000Z");
    DateTime expectedBatch = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expectedBatch);
    addSegments(initSegment);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    segmentAddedLatch = new CountDownLatch(initBatchSegments.size() + initGaplessRealtimeSegments.size());
    addSegments(initBatchSegments, 1);
    addRealtimeSegments(
        initGaplessRealtimeSegments,
        false,
        initBatchSegments.size() + 1
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        DateTimes.of("2011-04-13T00:00:00.000Z"),
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        DateTimes.of("2011-04-13T00:00:00.000Z"),
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        DateTimes.of("2011-04-09T00:00:00.000Z"),
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  @Test
  public void testRealtimeSegmentInitializationWithGaps() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    DateTime expected = DateTimes.of("2011-04-13T00:00:00.000Z");
    DateTime expectedComplete = DateTimes.of("2011-04-10T00:00:00.000Z");
    DateTime expectedBatch = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expectedComplete);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expectedBatch);
    addSegments(initSegment);

    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    segmentAddedLatch = new CountDownLatch(initBatchSegments.size() + initGapRealtimeSegments.size());
    addSegments(initBatchSegments, 1);
    addRealtimeSegments(
        initGapRealtimeSegments,
        false,
        initBatchSegments.size() + 1
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedComplete,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedBatch,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  @Test
  public void testRealtimeSegmentInitializationWithGapFromBatch() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    DateTime expected = DateTimes.of("2011-04-13T00:00:00.000Z");
    DateTime expectedBatch = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expectedBatch);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expectedBatch);
    addSegments(initSegment);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    segmentAddedLatch = new CountDownLatch(initBatchSegments.size() + initStartGapRealtimeSegments.size());
    addSegments(initBatchSegments, 1);
    addRealtimeSegments(
        initStartGapRealtimeSegments,
        false,
        initBatchSegments.size() + 1
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedBatch,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedBatch,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  @Test
  public void testRealtimeSegmentInitializationWithActiveIndexing() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    DateTime expected = DateTimes.of("2011-04-13T00:00:00.000Z");
    DateTime expectedBatch = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expectedBatch);

    addSegments(initSegment);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    segmentAddedLatch = new CountDownLatch(initBatchSegments.size() + initGaplessRealtimeSegments.size() + 2);

    addSegments(initBatchSegments, 1);
    addRealtimeSegments(
        initGaplessRealtimeSegments,
        false,
        initBatchSegments.size() + 1
    );
    addRealtimeSegments(
        ImmutableList.of(
            "2011-04-13T00:00:00.000Z/2011-04-14T00:00:00.000Z",
            "2011-04-14T00:00:00.000Z/2011-04-15T00:00:00.000Z"
        ),
        true,
        initBatchSegments.size() + initGaplessRealtimeSegments.size() + 1
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedBatch,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  // view created after segments are announced
  @Test
  public void testBatchSegmentAdded() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(initBatchSegments.size());
    segmentRemovedLatch = new CountDownLatch(0);
    addSegments(initBatchSegments);

    DateTime expected = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expected);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );

    segmentAddedLatch = new CountDownLatch(2);
    expected = DateTimes.of("2011-04-11T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expected);

    addSegments(
        ImmutableList.of(
            Pair.of("2011-04-09T00:00:00.000Z/2011-04-10T00:00:00.000Z", "v1"),
            Pair.of("2011-04-10T00:00:00.000Z/2011-04-11T00:00:00.000Z", "v1")
        ),
        initBatchSegments.size()
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  // view created after segments are announced
  @Test
  public void testBatchSegmentAddedWithGap() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(initBatchSegments.size());
    segmentRemovedLatch = new CountDownLatch(0);

    addSegments(initBatchSegments);

    DateTime expected = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expected);

    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );

    segmentAddedLatch = new CountDownLatch(2);
    expected = DateTimes.of("2011-04-12T00:00:00.000Z");
    DateTime expectedComplete = DateTimes.of("2011-04-10T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expectedComplete);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expectedComplete);

    addSegments(
        ImmutableList.of(
            Pair.of("2011-04-09T00:00:00.000Z/2011-04-10T00:00:00.000Z", "v1"),
            Pair.of("2011-04-11T00:00:00.000Z/2011-04-12T00:00:00.000Z", "v1")
        ),
        initBatchSegments.size()
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedComplete,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedComplete,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  // view created after segments are announced
  @Test
  public void testRealtimeSegmentAdded() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(initBatchSegments.size());
    segmentRemovedLatch = new CountDownLatch(0);

    addSegments(initBatchSegments);

    DateTime expected = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expected);

    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );

    segmentAddedLatch = new CountDownLatch(initGaplessRealtimeSegments.size());
    DateTime expectedUpdated = DateTimes.of("2011-04-13T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expectedUpdated);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expectedUpdated);

    addRealtimeSegments(
        initGaplessRealtimeSegments,
        false,
        initBatchSegments.size()
    );
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));

    Assert.assertEquals(
        expectedUpdated,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedUpdated,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  // view created after segments are announced
  @Test
  public void testRealtimeSegmentAddedWithGap() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(initBatchSegments.size());
    segmentRemovedLatch = new CountDownLatch(0);

    addSegments(initBatchSegments);

    DateTime expected = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expected);

    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );

    segmentAddedLatch = new CountDownLatch(initGapRealtimeSegments.size());
    DateTime expectedUpdated = DateTimes.of("2011-04-13T00:00:00.000Z");
    DateTime expectedUpdatedComplete = DateTimes.of("2011-04-10T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expectedUpdated);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expectedUpdatedComplete);

    addRealtimeSegments(
        initGapRealtimeSegments,
        false,
        initGapRealtimeSegments.size()
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));

    Assert.assertEquals(
        expectedUpdated,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedUpdatedComplete,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }


  @Test
  public void testRealtimeIndexingHandoffToHistorical() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    DateTime expected = DateTimes.of("2011-04-11T00:00:00.000Z");
    DateTime expectedBatch = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expectedBatch);
    addSegments(initSegment);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    segmentAddedLatch = new CountDownLatch(initBatchSegments.size() + 4);
    final List<Pair<DruidServer, DataSegment>> segments = addSegments(
        initBatchSegments
    );

    final List<Pair<DruidServer, DataSegment>> realtime = addRealtimeSegments(
        ImmutableList.of(
            "2011-04-09T00:00:00.000Z/2011-04-10T00:00:00.000Z",
            "2011-04-10T00:00:00.000Z/2011-04-11T00:00:00.000Z"
        ),
        false,
        segments.size()
    );

    final List<Pair<DruidServer, DataSegment>> realtimeStillIndexing = addRealtimeSegments(
        ImmutableList.of(
            "2011-04-11T00:00:00.000Z/2011-04-12T00:00:00.000Z",
            "2011-04-12T00:00:00.000Z/2011-04-13T00:00:00.000Z"
        ),
        true,
        segments.size() + realtime.size()
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedBatch,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );


    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(1);
    expected = DateTimes.of("2011-04-12T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);

    //unannounce active realtime indexing segment 2011-04-11/2011-04-12 and announce on historical
    unannounceSegmentForServer(realtimeStillIndexing.get(0).lhs, realtimeStillIndexing.get(0).rhs, zkPathsConfig);

    addRealtimeSegments(
        ImmutableList.of("2011-04-11T00:00:00.000Z/2011-04-12T00:00:00.000Z"),
        false,
        segments.size() + realtime.size() + realtimeStillIndexing.size()
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedBatch,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  @Test
  public void testRealtimeIndexingHandoffToHistoricalGap() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    DateTime expected = DateTimes.of("2011-04-11T00:00:00.000Z");
    DateTime expectedBatch = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expectedBatch);
    addSegments(initSegment);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    segmentAddedLatch = new CountDownLatch(9);
    final List<Pair<DruidServer, DataSegment>> segments = addSegments(
        ImmutableList.of(
            Pair.of("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z", "v1"),
            Pair.of("2011-04-03T00:00:00.000Z/2011-04-06T00:00:00.000Z", "v1"),
            Pair.of("2011-04-01T00:00:00.000Z/2011-04-09T00:00:00.000Z", "v2"),
            Pair.of("2011-04-07T00:00:00.000Z/2011-04-09T00:00:00.000Z", "v3"),
            Pair.of("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z", "v3")
        )
    );

    final List<Pair<DruidServer, DataSegment>> realtime = addRealtimeSegments(
        ImmutableList.of(
            "2011-04-09T00:00:00.000Z/2011-04-10T00:00:00.000Z",
            "2011-04-10T00:00:00.000Z/2011-04-11T00:00:00.000Z"
        ),
        false,
        segments.size()
    );

    final List<Pair<DruidServer, DataSegment>> realtimeStillIndexing = addRealtimeSegments(
        ImmutableList.of("2011-04-12T00:00:00.000Z/2011-04-13T00:00:00.000Z", "2011-04-13/2011-04-14T00:00:00.000Z"),
        true,
        segments.size() + realtime.size()
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedBatch,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );


    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(1);
    expected = DateTimes.of("2011-04-12T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);

    //unannounce active realtime indexing segment 2011-04-11/2011-04-12 and announce on historical
    unannounceSegmentForServer(realtimeStillIndexing.get(0).lhs, realtimeStillIndexing.get(0).rhs, zkPathsConfig);

    addRealtimeSegments(
        ImmutableList.of("2011-04-11T00:00:00.000Z/2011-04-12T00:00:00.000Z"),
        false,
        segments.size() + realtime.size() + realtimeStillIndexing.size()
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));

    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        expected,
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        expectedBatch,
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }

  @Test
  public void testWatermarksDoNotGoBackwardsEvenInOutage() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    DateTime expected = DateTimes.of("2011-04-11T00:00:00.000Z");
    DateTime expectedBatch = DateTimes.of("2011-04-09T00:00:00.000Z");
    addConditionLatch(StableDataHighWatermark.TYPE, expected);
    addConditionLatch(DataCompletenessLowWatermark.TYPE, expected);
    addConditionLatch(BatchCompletenessLowWatermark.TYPE, expectedBatch);
    addSegments(initSegment);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    segmentAddedLatch = new CountDownLatch(9);
    final List<Pair<DruidServer, DataSegment>> batchSegments = addSegments(
        ImmutableList.of(
            Pair.of("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z", "v1"),
            Pair.of("2011-04-03T00:00:00.000Z/2011-04-06T00:00:00.000Z", "v1"),
            Pair.of("2011-04-01T00:00:00.000Z/2011-04-09T00:00:00.000Z", "v2"),
            Pair.of("2011-04-07T00:00:00.000Z/2011-04-09T00:00:00.000Z", "v3"),
            Pair.of("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z", "v3")
        )
    );


    final List<Pair<DruidServer, DataSegment>> realtime = addRealtimeSegments(
        ImmutableList.of(
            "2011-04-09T00:00:00.000Z/2011-04-10T00:00:00.000Z",
            "2011-04-10T00:00:00.000Z/2011-04-11T00:00:00.000Z"
        ),
        false,
        batchSegments.size()
    );

    final List<Pair<DruidServer, DataSegment>> realtimeStillIndexing = addRealtimeSegments(
        ImmutableList.of(
            "2011-04-11T00:00:00.000Z/2011-04-12T00:00:00.000Z",
            "2011-04-12T00:00:00.000Z/2011-04-13T00:00:00.000Z"
        ),
        true,
        batchSegments.size() + realtime.size()
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(DataCompletenessLowWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(StableDataHighWatermark.TYPE)));
    Assert.assertTrue(timing.forWaiting().awaitLatch(getConditionLatch(BatchCompletenessLowWatermark.TYPE)));

    Assert.assertEquals(
        DateTimes.of("2011-04-11T00:00:00.000Z"),
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        DateTimes.of("2011-04-11T00:00:00.000Z"),
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        DateTimes.of("2011-04-09T00:00:00.000Z"),
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );


    segmentRemovedLatch = new CountDownLatch(4);

    //unannounce active realtime indexing segment 2011-04-11/2011-04-12 and announce on historical
    unannounceSegmentForServer(realtimeStillIndexing.get(0).lhs, realtimeStillIndexing.get(0).rhs, zkPathsConfig);
    unannounceSegmentForServer(realtime.get(0).lhs, realtime.get(0).rhs, zkPathsConfig);
    unannounceSegmentForServer(batchSegments.get(0).lhs, batchSegments.get(0).rhs, zkPathsConfig);
    unannounceSegmentForServer(batchSegments.get(2).lhs, batchSegments.get(2).rhs, zkPathsConfig);


    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));

    Thread.sleep(DELAY_MILLIS);
    Assert.assertEquals(
        DateTimes.of("2011-04-11T00:00:00.000Z"),
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        DateTimes.of("2011-04-11T00:00:00.000Z"),
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        DateTimes.of("2011-04-09T00:00:00.000Z"),
        timelineStore.getValue(testDatasource, BatchCompletenessLowWatermark.TYPE)
    );
  }
}
