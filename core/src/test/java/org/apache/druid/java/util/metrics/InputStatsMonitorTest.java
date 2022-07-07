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

package org.apache.druid.java.util.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InputStatsMonitorTest
{
  private InputStatsMonitor inputStatsMonitor;
  private InputStats inputStats;
  private Map<String, String[]> dimensions;
  private List<Event> events;
  private ServiceEmitter emitter;

  @Before
  public void setUp()
  {
    inputStats = new InputStats();
    dimensions = ImmutableMap.of("k1", new String[]{"v1"});
    events = new ArrayList<>();
    inputStatsMonitor = new InputStatsMonitor(inputStats, dimensions);
    emitter = new ServiceEmitter("", "", new TestEmitter(events));
  }

  @Test
  public void testInputStatsMonitor()
  {
    inputStats.incrementProcessedBytes(10);
    inputStatsMonitor.doMonitor(emitter);
    Assert.assertEquals(10L, events.get(0).toMap().get("value"));
    Assert.assertEquals("v1", ((List) events.get(0).toMap().get("k1")).get(0));
    inputStats.incrementProcessedBytes(100);
    inputStatsMonitor.doMonitor(emitter);
    Assert.assertEquals(100L, events.get(1).toMap().get("value"));
  }

  static class TestEmitter extends NoopEmitter
  {
    private final List<Event> events;

    public TestEmitter(List<Event> events)
    {
      this.events = events;
    }

    @Override
    public void emit(Event event)
    {
      events.add(event);
    }

    public List<Event> getEvents()
    {
      return events;
    }
  }
}
