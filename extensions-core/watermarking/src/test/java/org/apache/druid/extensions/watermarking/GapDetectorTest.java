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
import org.apache.druid.extensions.timeline.metadata.TimelineMetadata;
import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCollector;
import org.apache.druid.extensions.watermarking.gaps.BatchGapDetector;
import org.apache.druid.extensions.watermarking.gaps.BatchGapDetectorFactory;
import org.apache.druid.extensions.watermarking.gaps.GapDetectorFactory;
import org.apache.druid.extensions.watermarking.gaps.SegmentGapDetector;
import org.apache.druid.extensions.watermarking.gaps.SegmentGapDetectorFactory;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class GapDetectorTest extends TimelineMetadataCollectorTestBase
{
  final List<? extends GapDetectorFactory> gapDetectorFactories = ImmutableList.of(
      new SegmentGapDetectorFactory(),
      new BatchGapDetectorFactory()
  );

  @Test
  public void testNoGaps() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    segmentAddedLatch = new CountDownLatch(initBatchSegments.size());
    addSegments(initBatchSegments);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineMetadataCollector<List<Interval>> gapDetector =
        new TimelineMetadataCollector<>(
            timelineMetadataCollectorServerView,
            gapDetectorFactories
        );

    List<TimelineMetadata<List<Interval>>> results =
        gapDetector.fold(testDatasource, new Interval(DateTimes.utc(0), DateTimes.nowUtc()), true);

    for (TimelineMetadata<List<Interval>> result : results) {
      List<Interval> gaps = result.getMetadata();
      Assert.assertEquals(0, gaps.size());
    }
  }

  @Test
  public void testTotalGap() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    segmentAddedLatch = new CountDownLatch(initBatchSegmentsWithGap.size());
    addSegments(initBatchSegmentsWithGap);
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineMetadataCollector<List<Interval>> gapDetector =
        new TimelineMetadataCollector<>(
            timelineMetadataCollectorServerView,
            gapDetectorFactories
        );

    List<TimelineMetadata<List<Interval>>> results =
        gapDetector.fold(testDatasource, new Interval(DateTimes.utc(0), DateTimes.nowUtc()), true);

    for (TimelineMetadata<List<Interval>> result : results) {
      List<Interval> gaps = result.getMetadata();

      Assert.assertEquals(1, gaps.size());
      Assert.assertEquals(gaps.get(0), Intervals.of("2011-04-06T00:00:00Z/2011-04-07T00:00:00Z"));
    }
  }

  @Test
  public void testBatchOnlyGap() throws Exception
  {
    List<String> realtimeIndexed = ImmutableList.of(
        "2011-04-01T00:00:00Z/2011-04-03T00:00:00Z",
        "2011-04-03T00:00:00Z/2011-04-06T00:00:00Z",
        "2011-04-06T00:00:00Z/2011-04-09T00:00:00Z"
    );

    segmentViewInitLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    segmentAddedLatch = new CountDownLatch(initBatchSegmentsWithGap.size()
                                           + realtimeIndexed.size()
                                           + initGaplessRealtimeSegments.size());
    addSegments(initBatchSegmentsWithGap);

    addRealtimeSegments(
        realtimeIndexed,
        false,
        initBatchSegmentsWithGap.size()
    );
    addRealtimeSegments(
        initGaplessRealtimeSegments,
        false,
        initBatchSegmentsWithGap.size() + realtimeIndexed.size()
    );
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineMetadataCollector<List<Interval>> gapDetector =
        new TimelineMetadataCollector<>(
            timelineMetadataCollectorServerView,
            gapDetectorFactories
        );

    List<TimelineMetadata<List<Interval>>> detectorResults =
        gapDetector.fold(testDatasource, new Interval(DateTimes.utc(0), DateTimes.nowUtc()), true);

    for (TimelineMetadata<List<Interval>> detectorMetadata : detectorResults) {
      List<Interval> gaps = detectorMetadata.getMetadata();

      if (detectorMetadata.getType() == SegmentGapDetector.GAP_TYPE) {
        Assert.assertEquals(0, gaps.size());
      } else if (detectorMetadata.getType() == BatchGapDetector.GAP_TYPE) {
        Assert.assertEquals(1, gaps.size());
        Assert.assertEquals(gaps.get(0), Intervals.of("2011-04-06T00:00:00Z/2011-04-07T00:00:00Z"));
      }
    }
  }

  @Test
  public void testMixedGaps() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    segmentAddedLatch = new CountDownLatch(initBatchSegmentsWithGap.size() + initGapRealtimeSegments.size());
    addSegments(initBatchSegmentsWithGap);

    addRealtimeSegments(
        initGapRealtimeSegments,
        false,
        initBatchSegmentsWithGap.size()
    );
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineMetadataCollector<List<Interval>> gapDetector =
        new TimelineMetadataCollector<>(
            timelineMetadataCollectorServerView,
            gapDetectorFactories
        );

    List<TimelineMetadata<List<Interval>>> detectorResults =
        gapDetector.fold(testDatasource, new Interval(DateTimes.utc(0), DateTimes.nowUtc()), true);

    for (TimelineMetadata<List<Interval>> detectorMetadata : detectorResults) {
      List<Interval> gaps = detectorMetadata.getMetadata();

      if (detectorMetadata.getType() == SegmentGapDetector.GAP_TYPE) {
        Assert.assertEquals(2, gaps.size());
        Assert.assertEquals(gaps.get(0), Intervals.of("2011-04-06T00:00:00Z/2011-04-07T00:00:00Z"));
        Assert.assertEquals(gaps.get(1), Intervals.of("2011-04-10T00:00:00Z/2011-04-11T00:00:00Z"));
      } else if (detectorMetadata.getType() == BatchGapDetector.GAP_TYPE) {
        Assert.assertEquals(1, gaps.size());
        Assert.assertEquals(gaps.get(0), Intervals.of("2011-04-06T00:00:00Z/2011-04-07T00:00:00Z"));
      }
    }
  }

  @Test
  public void testMoreMixedGaps() throws Exception
  {

    List<String> realtimeIndexed = ImmutableList.of(
        "2011-04-01T00:00:00Z/2011-04-03T00:00:00Z",
        "2011-04-03T00:00:00Z/2011-04-06T00:00:00Z",
        "2011-04-06T00:00:00Z/2011-04-09T00:00:00Z",
        "2011-04-11T00:00:00Z/2011-04-12T00:00:00Z",
        "2011-04-12T00:00:00Z/2011-04-13T00:00:00Z",
        "2011-04-14T00:00:00Z/2011-04-15T00:00:00Z",
        "2011-04-15T00:00:00Z/2011-04-16T00:00:00Z"
    );
    List<String> batchIndexed = ImmutableList.of(
        "2011-04-01T00:00:00Z/2011-04-03T00:00:00Z",
        "2011-04-03T00:00:00Z/2011-04-05T00:00:00Z",
        "2011-04-06T00:00:00Z/2011-04-09T00:00:00Z",
        "2011-04-09T00:00:00Z/2011-04-11T00:00:00Z",
        "2011-04-12T00:00:00Z/2011-04-13T00:00:00Z",
        "2011-04-14T00:00:00Z/2011-04-15T00:00:00Z"
    );


    segmentViewInitLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(0);
    segmentAddedLatch = new CountDownLatch(batchIndexed.size()
                                           + realtimeIndexed.size());
    addSegments(batchIndexed.stream().map(s -> new Pair<>(s, "v1")).collect(Collectors.toList()));

    addRealtimeSegments(
        realtimeIndexed,
        false,
        batchIndexed.size()
    );
    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineMetadataCollector<List<Interval>> gapDetector =
        new TimelineMetadataCollector<>(
            timelineMetadataCollectorServerView,
            gapDetectorFactories
        );

    List<TimelineMetadata<List<Interval>>> detectorResults =
        gapDetector.fold(testDatasource, new Interval(DateTimes.utc(0), DateTimes.nowUtc()), true);

    for (TimelineMetadata<List<Interval>> detectorMetadata : detectorResults) {
      List<Interval> gaps = detectorMetadata.getMetadata();

      if (detectorMetadata.getType() == SegmentGapDetector.GAP_TYPE) {
        Assert.assertEquals(1, gaps.size());
        Assert.assertEquals(gaps.get(0), Intervals.of("2011-04-13T00:00:00Z/2011-04-14T00:00:00Z"));
      } else if (detectorMetadata.getType() == BatchGapDetector.GAP_TYPE) {
        Assert.assertEquals(3, gaps.size());
        Assert.assertEquals(gaps.get(0), Intervals.of("2011-04-05T00:00:00Z/2011-04-06T00:00:00Z"));
        Assert.assertEquals(gaps.get(1), Intervals.of("2011-04-11T00:00:00Z/2011-04-12T00:00:00Z"));
        Assert.assertEquals(gaps.get(2), Intervals.of("2011-04-13T00:00:00Z/2011-04-14T00:00:00Z"));
      }
    }
  }
}
