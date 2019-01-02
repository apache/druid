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

import org.apache.druid.extensions.watermarking.http.WatermarkKeeperResource;
import org.apache.druid.extensions.watermarking.watermarks.BatchCompletenessLowWatermark;
import org.apache.druid.extensions.watermarking.watermarks.DataCompletenessLowWatermark;
import org.apache.druid.extensions.watermarking.watermarks.StableDataHighWatermark;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class WatermarkKeeperResourceTest extends TimelineMetadataCollectorTestBase
{
  private WatermarkKeeperResource resource;
  private String testDatasource1 = "test1";
  private String testDatasource2 = "test2";
  private DateTime t1;
  private DateTime t2;
  private DateTime t3;
  private DateTime t4;
  private DateTime t5;

  public WatermarkKeeperResourceTest() throws Exception
  {
    super();
  }

  @Before
  @Override
  public void setUp() throws Exception
  {
    setupStore();
    t1 = DateTimes.nowUtc();
    t2 = t1.minusMinutes(15);
    t3 = t1.minusHours(10);
    t4 = t1.minusMinutes(15);
    t5 = t1.minusHours(5);

    timelineStore.update(testDatasource1, StableDataHighWatermark.TYPE, t1);
    timelineStore.update(testDatasource1, DataCompletenessLowWatermark.TYPE, t2);
    timelineStore.update(testDatasource1, BatchCompletenessLowWatermark.TYPE, t3);
    timelineStore.update(testDatasource2, StableDataHighWatermark.TYPE, t4);
    timelineStore.update(testDatasource2, DataCompletenessLowWatermark.TYPE, t4);
    timelineStore.update(testDatasource2, BatchCompletenessLowWatermark.TYPE, t5);
    resource = new WatermarkKeeperResource(timelineStore, timelineStore);
  }

  @After
  @Override
  public void tearDown()
  {

  }

  @Test
  public void testGetDatasource() throws Exception
  {

    Map<String, DateTime> ds1 = (Map<String, DateTime>) resource.getDatasource(testDatasource1).getEntity();
    Map<String, DateTime> ds2 = (Map<String, DateTime>) resource.getDatasource(testDatasource2).getEntity();

    Assert.assertEquals(t1, ds1.get(StableDataHighWatermark.TYPE));
    Assert.assertEquals(t2, ds1.get(DataCompletenessLowWatermark.TYPE));
    Assert.assertEquals(t3, ds1.get(BatchCompletenessLowWatermark.TYPE));
    Assert.assertEquals(t4, ds2.get(StableDataHighWatermark.TYPE));
    Assert.assertEquals(t4, ds2.get(DataCompletenessLowWatermark.TYPE));
    Assert.assertEquals(t5, ds2.get(BatchCompletenessLowWatermark.TYPE));
  }

  @Test
  public void testGetValues() throws Exception
  {
    Assert.assertEquals(
        t1,
        ((Map<String, DateTime>) resource.getDatasourceKey(testDatasource1, StableDataHighWatermark.TYPE).getEntity())
            .get(StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        t2,
        ((Map<String, DateTime>) resource.getDatasourceKey(testDatasource1, DataCompletenessLowWatermark.TYPE)
                                         .getEntity())
            .get(DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        t3,
        ((Map<String, DateTime>) resource.getDatasourceKey(testDatasource1, BatchCompletenessLowWatermark.TYPE)
                                         .getEntity())
            .get(BatchCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        t4,
        ((Map<String, DateTime>) resource.getDatasourceKey(testDatasource2, StableDataHighWatermark.TYPE).getEntity())
            .get(StableDataHighWatermark.TYPE)
    );
    Assert.assertEquals(
        t4,
        ((Map<String, DateTime>) resource.getDatasourceKey(testDatasource2, DataCompletenessLowWatermark.TYPE)
                                         .getEntity())
            .get(DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertEquals(
        t5,
        ((Map<String, DateTime>) resource.getDatasourceKey(testDatasource2, BatchCompletenessLowWatermark.TYPE)
                                         .getEntity())
            .get(BatchCompletenessLowWatermark.TYPE)
    );
  }

  @Test
  public void testHistory() throws Exception
  {
    Thread.sleep(100);
    List<Map<String, DateTime>> timeline = (List<Map<String, DateTime>>) resource.getDatasourceHistory(
        testDatasource1,
        StableDataHighWatermark.TYPE,
        t5.toString(ISODateTimeFormat.dateTime()),
        DateTimes.nowUtc().toString(ISODateTimeFormat.dateTime())
    ).getEntity();
    Assert.assertEquals(1, timeline.size());
  }
}
