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

package org.apache.druid.query.metrics;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.MultiQueryMetricsCollector;
import org.apache.druid.query.SingleQueryMetricsCollector;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class QueryMetricsTest
{
  @Test
  public void testSingleMetrics()
  {
    SingleQueryMetricsCollector coll = SingleQueryMetricsCollector
        .newCollector()
        .setSubQueryId("query-id")
        .addCurrentThread()
        .addSegment()
        .addSegmentProcessed()
        .addSegmentProcessed()
        .addSegmentVectorProcessed()
        .addSegmentVectorProcessed()
        .addSegmentVectorProcessed()
        .addPreFilteredRows(10)
        .addNodeRows(20)
        .setResultRows(30)
        .addCpuNanos(40)
        .setQueryStart(50)
        .setQueryMs(60)
        .addSegmentRows(70);

    assertEquals("query-id", coll.getSubQueryId());
    assertEquals(1, coll.getSegments());
    assertEquals(2, coll.getSegmentsProcessed());
    assertEquals(3, coll.getSegmentsVectorProcessed());
    assertEquals(10, coll.getPreFilteredRows());
    assertEquals(20, coll.getNodeRows());
    assertEquals(30, coll.getResultRows());
    assertEquals(40, coll.getCpuNanos());
    assertEquals(50, coll.getQueryStart());
    assertEquals(60, coll.getQueryMs());
    assertEquals(70, coll.getSegmentRows());
    // Kinda weird, but the count is zero for a "live" collector.
    assertEquals(0, coll.getThreads());
    assertEquals(1, coll.getThreadsSnapshot());
  }

  @Test
  public void testSingleMetricsAdd()
  {
    SingleQueryMetricsCollector coll = SingleQueryMetricsCollector
        .newCollector()
        .setSubQueryId("query-id")
        .addCurrentThread()
        .addSegment()
        .addSegmentProcessed()
        .addSegmentProcessed()
        .addSegmentVectorProcessed()
        .addSegmentVectorProcessed()
        .addSegmentVectorProcessed()
        .addPreFilteredRows(10)
        .addNodeRows(20)
        .setResultRows(30)
        .addCpuNanos(40)
        .setQueryStart(50)
        .setQueryMs(60)
        .addSegmentRows(70);
    SingleQueryMetricsCollector second = SingleQueryMetricsCollector
        .newCollector()
        .setSubQueryId("query-id")
        .addCurrentThread()
        .addSegment()
        .addSegmentProcessed()
        .addSegmentVectorProcessed()
        .addPreFilteredRows(110)
        .addNodeRows(120)
        .setResultRows(130)
        .addCpuNanos(140)
        .setQueryStart(150)
        .setQueryMs(160)
        .addSegmentRows(170);
    coll.add(second);

    assertEquals("query-id", coll.getSubQueryId());
    assertEquals(2, coll.getSegments());
    assertEquals(3, coll.getSegmentsProcessed());
    assertEquals(4, coll.getSegmentsVectorProcessed());
    assertEquals(120, coll.getPreFilteredRows());
    assertEquals(140, coll.getNodeRows());
    // Second set wins
    assertEquals(130, coll.getResultRows());
    assertEquals(180, coll.getCpuNanos());
    // Second set wins
    assertEquals(150, coll.getQueryStart());
    // Second set wins
    assertEquals(160, coll.getQueryMs());
    assertEquals(240, coll.getSegmentRows());
    // Not set yet
    assertEquals(0, coll.getThreads());
    // Both in same thread
    assertEquals(1, coll.getThreadsSnapshot());
  }

  @Test(expected = IAE.class)
  public void testMismatchedIds()
  {
    SingleQueryMetricsCollector coll = SingleQueryMetricsCollector
        .newCollector()
        .setSubQueryId("query-id");
    SingleQueryMetricsCollector second = SingleQueryMetricsCollector
        .newCollector()
        .setSubQueryId("different-id");
    coll.add(second);
  }

  @Test
  public void testMultiMetricsSameId()
  {
    SingleQueryMetricsCollector single = SingleQueryMetricsCollector
        .newCollector()
        .setSubQueryId("query-id")
        .addCurrentThread()
        .addNodeRows(20);

    MultiQueryMetricsCollector multi = MultiQueryMetricsCollector
        .newCollector()
        .add(single);
    assertEquals(1, multi.getCollectors().size());

    SingleQueryMetricsCollector second = SingleQueryMetricsCollector
        .newCollector()
        .setSubQueryId("query-id")
        .addNodeRows(120);

    multi.add(second);
    assertEquals(1, multi.getCollectors().size());
    SingleQueryMetricsCollector coll = multi.getCollectors().get(0);

    assertEquals("query-id", coll.getSubQueryId());
    assertEquals(140, coll.getNodeRows());

    MultiQueryMetricsCollector secondMulti = MultiQueryMetricsCollector
        .newCollector()
        .addAll(multi);
    assertEquals(1, secondMulti.getCollectors().size());
    coll = secondMulti.getCollectors().get(0);

    assertEquals("query-id", coll.getSubQueryId());
    assertEquals(140, coll.getNodeRows());
  }

  @Test
  public void testMultiMetricsSubQuery()
  {
    SingleQueryMetricsCollector root = SingleQueryMetricsCollector
        .newCollector()
        .setSubQueryId("query-id")
        .addNodeRows(20);
    SingleQueryMetricsCollector subquery = SingleQueryMetricsCollector
        .newCollector()
        .setSubQueryId("query-id_1")
        .addNodeRows(20);

    MultiQueryMetricsCollector multi = MultiQueryMetricsCollector
        .fromList(Arrays.asList(root, subquery));
    assertEquals(2, multi.getCollectors().size());
  }
}
