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

package org.apache.druid.query.scan;

import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

public class ScanQueryQueryToolChestTest
{
  private static ScanQueryQueryToolChest chest;
  private static ScanQueryConfig config;
  private static int numElements;
  private static QuerySegmentSpec emptySegmentSpec;

  @BeforeClass
  public void setup()
  {
    config = createNiceMock(ScanQueryConfig.class);
    expect(config.getMaxRowsTimeOrderedInMemory()).andReturn(100000);
    replay(config);
    chest = new ScanQueryQueryToolChest(config, null);
    numElements = 1000;
    emptySegmentSpec = new QuerySegmentSpec()
    {
      @Override
      public List<Interval> getIntervals()
      {
        return null;
      }

      @Override
      public <T> QueryRunner<T> lookup(
          Query<T> query,
          QuerySegmentWalker walker
      )
      {
        return null;
      }
    };
  }

  @Test
  public void testDescendingHeapsortListScanResultValues()
  {
    List<ScanResultValue> inputs = new ArrayList<>();
    for (long i = 0; i < numElements; i++) {
      HashMap<String, Object> event = new HashMap<>();
      event.put("__time", i * 1000);
      inputs.add(
          new ScanResultValue(
              "some segment id",
              Collections.singletonList("__time"),
              Collections.singletonList(event)
          )
      );
    }
    ScanQuery scanQuery = new Druids.ScanQueryBuilder()
        .resultFormat("list")
        .timeOrder(ScanQuery.TIME_ORDER_DESCENDING)
        .dataSource("some data source")
        .intervals(emptySegmentSpec)
        .limit(99999)
        .build();
    Iterator<ScanResultValue> sorted = chest.heapsortScanResultValues(inputs.iterator(), scanQuery);

    int count = 0;
    Long previousTime = Long.MAX_VALUE;
    while (sorted.hasNext()) {
      count++;
      ScanResultValue curr = sorted.next();
      Long currentTime = (Long)
          ((Map<String, Object>) (((List<Object>) curr.getEvents()).get(0))).get(ColumnHolder.TIME_COLUMN_NAME);
      Assert.assertTrue(currentTime < previousTime);
      previousTime = currentTime;
    }
    Assert.assertEquals(numElements, count);
  }

  @Test
  public void testAscendingHeapsortListScanResultValues()
  {
    List<ScanResultValue> inputs = new ArrayList<>();
    for (long i = numElements; i > 0; i--) {
      HashMap<String, Object> event = new HashMap<>();
      event.put("__time", i * 1000);
      inputs.add(
          new ScanResultValue(
              "some segment id",
              Collections.singletonList("__time"),
              Collections.singletonList(event)
          )
      );
    }
    ScanQuery scanQuery = new Druids.ScanQueryBuilder()
        .resultFormat("list")
        .timeOrder(ScanQuery.TIME_ORDER_ASCENDING)
        .dataSource("some data source")
        .intervals(emptySegmentSpec)
        .limit(99999)
        .build();
    Iterator<ScanResultValue> sorted = chest.heapsortScanResultValues(inputs.iterator(), scanQuery);

    int count = 0;
    Long previousTime = -1L;
    while (sorted.hasNext()) {
      count++;
      ScanResultValue curr = sorted.next();
      Long currentTime = (Long)
          ((Map<String, Object>) (((List<Object>) curr.getEvents()).get(0))).get(ColumnHolder.TIME_COLUMN_NAME);
      Assert.assertTrue(currentTime > previousTime);
      previousTime = currentTime;
    }
    Assert.assertEquals(numElements, count);
  }

  @Test
  public void testDescendingHeapsortCompactedListScanResultValues()
  {
    List<ScanResultValue> inputs = new ArrayList<>();
    for (long i = 0; i < numElements; i++) {
      inputs.add(
          new ScanResultValue(
              "some segment id",
              Collections.singletonList("__time"),
              Collections.singletonList(Collections.singletonList(new Long(i * 1000)))
          )
      );
    }
    ScanQuery scanQuery = new Druids.ScanQueryBuilder()
        .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
        .timeOrder(ScanQuery.TIME_ORDER_DESCENDING)
        .dataSource("some data source")
        .intervals(emptySegmentSpec)
        .limit(99999)
        .build();
    Iterator<ScanResultValue> sorted = chest.heapsortScanResultValues(inputs.iterator(), scanQuery);

    Long previousTime = Long.MAX_VALUE;
    int count = 0;
    while (sorted.hasNext()) {
      count++;
      ScanResultValue curr = sorted.next();
      Long currentTime = (Long)
          ((List<Object>) (((List<Object>) curr.getEvents()).get(0))).get(0);
      Assert.assertTrue(currentTime < previousTime);
      previousTime = currentTime;
    }
    Assert.assertEquals(numElements, count);
  }

  @Test
  public void testAscendingHeapsortCompactedListScanResultValues()
  {
    List<ScanResultValue> inputs = new ArrayList<>();
    for (long i = numElements; i > 0; i--) {
      inputs.add(
          new ScanResultValue(
              "some segment id",
              Collections.singletonList("__time"),
              Collections.singletonList(Collections.singletonList(new Long(i * 1000)))
          )
      );
    }
    ScanQuery scanQuery = new Druids.ScanQueryBuilder()
        .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
        .timeOrder(ScanQuery.TIME_ORDER_ASCENDING)
        .dataSource("some data source")
        .intervals(emptySegmentSpec)
        .limit(99999)
        .build();
    Iterator<ScanResultValue> sorted = chest.heapsortScanResultValues(inputs.iterator(), scanQuery);

    Long previousTime = -1L;
    int count = 0;
    while (sorted.hasNext()) {
      count++;
      ScanResultValue curr = sorted.next();
      Long currentTime = (Long)
          ((List<Object>) (((List<Object>) curr.getEvents()).get(0))).get(0);
      Assert.assertTrue(currentTime > previousTime);
      previousTime = currentTime;
    }
    Assert.assertEquals(numElements, count);
  }
}
