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

package org.apache.druid.server.metrics;

import com.google.common.base.Supplier;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryBufferPoolStatsMonitorTest
{
  private BlockingPool<ByteBuffer> mergeBufferPool;
  private NonBlockingPool<ByteBuffer> intermediateResultPool;

  @Before
  public void setUp()
  {
    Supplier<ByteBuffer> generator = new OffheapBufferGenerator("test", 100);

    mergeBufferPool = new DefaultBlockingPool<>(generator, 10);
    mergeBufferPool.takeBatch(3);

    intermediateResultPool = new StupidPool<>("intermediatePool", generator, 5, 6);
    intermediateResultPool.take();
  }

  @Test
  public void testMonitor()
  {
    final QueryBufferPoolStatsMonitor monitor = new QueryBufferPoolStatsMonitor(mergeBufferPool,
                                                                                intermediateResultPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Map<String, Object> resultMap = emitter.getEvents()
                                           .stream()
                                           .collect(Collectors.toMap(
                                               event -> (String) event.toMap().get("metric"),
                                               event -> (Object) event.toMap().get("value")
                                           ));
    Assert.assertEquals(3, resultMap.size());
    Assert.assertEquals(10, (int) resultMap.get("query/mergeBufferMaxNum"));
    Assert.assertEquals(7, (int) resultMap.get("query/mergeBufferAvailNum"));
    Assert.assertEquals(4L, (long) resultMap.get("query/intermResultBufferAvailNum"));
  }
}
