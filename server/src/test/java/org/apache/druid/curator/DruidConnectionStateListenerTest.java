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

package org.apache.druid.curator;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DruidConnectionStateListenerTest
{
  private TestEmitter emitter;
  private DruidConnectionStateListener listener;

  @Before
  public void setUp()
  {
    emitter = new TestEmitter();
    listener = new DruidConnectionStateListener(emitter);
  }

  @Test
  public void test_isConnected()
  {
    Assert.assertFalse(listener.isConnected());

    listener.stateChanged(null, ConnectionState.CONNECTED);
    Assert.assertTrue(listener.isConnected());

    listener.stateChanged(null, ConnectionState.SUSPENDED);
    Assert.assertFalse(listener.isConnected());

    listener.stateChanged(null, ConnectionState.RECONNECTED);
    Assert.assertTrue(listener.isConnected());

    listener.stateChanged(null, ConnectionState.LOST);
    Assert.assertFalse(listener.isConnected());
  }

  @Test
  public void test_doMonitor_init()
  {
    listener.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getEvents().size());

    final Map<String, Object> eventMap = emitter.getEvents().get(0).toMap();
    Assert.assertEquals("zk/connected", eventMap.get("metric"));
    Assert.assertEquals(0, eventMap.get("value"));
  }

  @Test
  public void test_doMonitor_connected()
  {
    listener.stateChanged(null, ConnectionState.CONNECTED);
    listener.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getEvents().size());

    final Map<String, Object> eventMap = emitter.getEvents().get(0).toMap();
    Assert.assertEquals("zk/connected", eventMap.get("metric"));
    Assert.assertEquals(1, eventMap.get("value"));
  }

  @Test
  public void test_doMonitor_notConnected()
  {
    listener.stateChanged(null, ConnectionState.SUSPENDED);
    listener.doMonitor(emitter);
    Assert.assertEquals(2, emitter.getEvents().size()); // 2 because stateChanged emitted an alert

    final Map<String, Object> eventMap = emitter.getEvents().get(1).toMap();
    Assert.assertEquals("zk/connected", eventMap.get("metric"));
    Assert.assertEquals(0, eventMap.get("value"));
  }

  @Test
  public void test_suspendedAlert()
  {
    listener.stateChanged(null, ConnectionState.SUSPENDED);
    Assert.assertEquals(1, emitter.getEvents().size());

    final Map<String, Object> alertMap = emitter.getEvents().get(0).toMap();
    Assert.assertEquals("alerts", alertMap.get("feed"));
    Assert.assertEquals("ZooKeeper connection[SUSPENDED]", alertMap.get("description"));
  }

  @Test
  public void test_reconnectedMetric()
  {
    listener.stateChanged(null, ConnectionState.SUSPENDED);
    Assert.assertEquals(1, emitter.getEvents().size()); // the first stateChanged emits an alert

    listener.stateChanged(null, ConnectionState.RECONNECTED);
    Assert.assertEquals(2, emitter.getEvents().size()); // the second stateChanged emits a metric

    final Map<String, Object> eventMap = emitter.getEvents().get(1).toMap();
    Assert.assertEquals("metrics", eventMap.get("feed"));
    Assert.assertEquals("zk/reconnect/time", eventMap.get("metric"));
    MatcherAssert.assertThat(eventMap.get("value"), CoreMatchers.instanceOf(Long.class));
    MatcherAssert.assertThat(((Number) eventMap.get("value")).longValue(), Matchers.greaterThanOrEqualTo(0L));
  }

  private static class TestEmitter extends NoopServiceEmitter
  {
    @GuardedBy("events")
    private final List<Event> events = new ArrayList<>();

    @Override
    public void emit(Event event)
    {
      synchronized (events) {
        events.add(event);
      }
    }

    public List<Event> getEvents()
    {
      synchronized (events) {
        return ImmutableList.copyOf(events);
      }
    }
  }
}
