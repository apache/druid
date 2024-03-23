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

import org.apache.curator.framework.state.ConnectionState;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DruidConnectionStateListenerTest
{
  private StubServiceEmitter emitter;
  private DruidConnectionStateListener listener;

  @Before
  public void setUp()
  {
    emitter = new StubServiceEmitter("DruidConnectionStateListenerTest", "localhost");
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
    emitter.verifyValue("zk/connected", 0);
  }

  @Test
  public void test_doMonitor_connected()
  {
    listener.stateChanged(null, ConnectionState.CONNECTED);
    listener.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getEvents().size());

    emitter.verifyValue("zk/connected", 1);
  }

  @Test
  public void test_doMonitor_notConnected()
  {
    listener.stateChanged(null, ConnectionState.SUSPENDED);
    listener.doMonitor(emitter);
    Assert.assertEquals(2, emitter.getEvents().size()); // 2 because stateChanged emitted an alert

    emitter.verifyValue("zk/connected", 0);
  }

  @Test
  public void test_suspendedAlert()
  {
    listener.stateChanged(null, ConnectionState.SUSPENDED);
    Assert.assertEquals(1, emitter.getEvents().size());

    final AlertEvent alert = emitter.getAlerts().get(0);
    Assert.assertEquals("alerts", alert.getFeed());
    Assert.assertEquals("ZooKeeper connection[SUSPENDED]", alert.getDescription());
  }

  @Test
  public void test_reconnectedMetric()
  {
    listener.stateChanged(null, ConnectionState.SUSPENDED);
    Assert.assertEquals(1, emitter.getEvents().size()); // the first stateChanged emits an alert

    listener.stateChanged(null, ConnectionState.RECONNECTED);
    Assert.assertEquals(2, emitter.getEvents().size()); // the second stateChanged emits a metric

    long observedReconnectTime = emitter.getValue("zk/reconnect/time", null).longValue();
    Assert.assertTrue(observedReconnectTime >= 0);
  }

}
