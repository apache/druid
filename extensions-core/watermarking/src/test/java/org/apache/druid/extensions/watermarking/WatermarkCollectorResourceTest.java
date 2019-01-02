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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.extensions.watermarking.gaps.BatchGapDetector;
import org.apache.druid.extensions.watermarking.gaps.BatchGapDetectorFactory;
import org.apache.druid.extensions.watermarking.gaps.SegmentGapDetector;
import org.apache.druid.extensions.watermarking.gaps.SegmentGapDetectorFactory;
import org.apache.druid.extensions.watermarking.http.WatermarkCollectorResource;
import org.apache.druid.extensions.watermarking.watermarks.BatchCompletenessLowWatermark;
import org.apache.druid.extensions.watermarking.watermarks.BatchCompletenessLowWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.BatchDataHighWatermark;
import org.apache.druid.extensions.watermarking.watermarks.BatchDataHighWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.DataCompletenessLowWatermark;
import org.apache.druid.extensions.watermarking.watermarks.DataCompletenessLowWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.MaxtimeWatermark;
import org.apache.druid.extensions.watermarking.watermarks.MaxtimeWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.StableDataHighWatermark;
import org.apache.druid.extensions.watermarking.watermarks.StableDataHighWatermarkFactory;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WatermarkCollectorResourceTest extends TimelineMetadataCollectorTestBase
{
  private WatermarkCollectorResource resource;

  private String testDatasource = "test1";
  private DateTime testTime;

  public WatermarkCollectorResourceTest() throws Exception
  {
    super();
  }

  @Before
  @Override
  public void setUp() throws Exception
  {
    setupStore();
    setupMockView();
    testTime = DateTimes.nowUtc().minusMinutes(15);
    timelineStore.update(testDatasource, DataCompletenessLowWatermark.TYPE, testTime);
    timelineStore.update(testDatasource, StableDataHighWatermark.TYPE, testTime);
    resource = new WatermarkCollectorResource(
        timelineMetadataCollectorServerView,
        timelineStore,
        timelineStore,
        ImmutableMap.of(
            BatchCompletenessLowWatermark.TYPE,
            new BatchCompletenessLowWatermarkFactory(cache, timelineStore),
            BatchDataHighWatermark.TYPE,
            new BatchDataHighWatermarkFactory(cache, timelineStore),
            DataCompletenessLowWatermark.TYPE,
            new DataCompletenessLowWatermarkFactory(cache, timelineStore),
            StableDataHighWatermark.TYPE,
            new StableDataHighWatermarkFactory(cache, timelineStore),
            MaxtimeWatermark.TYPE,
            new MaxtimeWatermarkFactory(cache, timelineStore)
        ),
        ImmutableMap.of(
            BatchGapDetector.GAP_TYPE, new BatchGapDetectorFactory(),
            SegmentGapDetector.GAP_TYPE, new SegmentGapDetectorFactory()
        )
    );
  }

  @After
  @Override
  public void tearDown()
  {

  }

  @Test
  public void testUpdate() throws Exception
  {
    Assert.assertEquals(testTime, timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE));
    DateTime now = DateTimes.nowUtc();
    resource.updateDatasourceKey(
        testDatasource,
        DataCompletenessLowWatermark.TYPE,
        now.toString(ISODateTimeFormat.dateTime())
    );
    Assert.assertEquals(
        now.minuteOfDay().roundCeilingCopy().getMillis(),
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE).getMillis()
    );
  }

  @Test
  public void testUpdateFails() throws Exception
  {
    Assert.assertEquals(testTime, timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE));
    DateTime now = DateTimes.nowUtc();
    resource.updateDatasourceKey(
        testDatasource,
        StableDataHighWatermark.TYPE,
        now.toString(ISODateTimeFormat.dateTime())
    );
    Assert.assertEquals(testTime, timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE));
  }


  @Test
  public void testRollback() throws Exception
  {
    DateTime now = DateTimes.nowUtc();
    DateTime fivemin = DateTimes.nowUtc().minusMinutes(5);
    timelineStore.update(testDatasource, DataCompletenessLowWatermark.TYPE, now);

    Assert.assertEquals(now, timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE));

    resource.rollback(
        testDatasource,
        DataCompletenessLowWatermark.TYPE,
        fivemin.toString(ISODateTimeFormat.dateTime())
    );
    Assert.assertEquals(testTime, timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE));
  }

  @Test
  public void testPurgeHistory() throws Exception
  {
    DateTime now = DateTimes.nowUtc();
    Thread.sleep(1000);
    DateTime later = DateTimes.nowUtc();
    timelineStore.update(testDatasource, DataCompletenessLowWatermark.TYPE, later);
    cache.set(testDatasource, DataCompletenessLowWatermark.TYPE, later);

    Thread.sleep(100);

    Assert.assertEquals(
        2,
        timelineStore.getValueHistory(
            testDatasource,
            DataCompletenessLowWatermark.TYPE,
            new Interval(DateTimes.utc(Long.MIN_VALUE), DateTimes.nowUtc())
        ).size()
    );

    resource.purge(
        testDatasource,
        DataCompletenessLowWatermark.TYPE,
        now.toString(ISODateTimeFormat.dateTime())
    );

    Assert.assertEquals(testTime, timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE));
    Assert.assertEquals(
        1,
        timelineStore.getValueHistory(
            testDatasource,
            DataCompletenessLowWatermark.TYPE,
            new Interval(DateTimes.utc(Long.MIN_VALUE), DateTimes.nowUtc())
        ).size()
    );
  }
}
