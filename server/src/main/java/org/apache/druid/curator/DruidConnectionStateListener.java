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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.druid.java.util.emitter.service.AlertBuilder;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

/**
 * Curator {@link ConnectionStateListener} that uses a {@link ServiceEmitter} to send alerts on ZK connection loss,
 * and emit metrics about ZK connection status.
 */
public class DruidConnectionStateListener extends AbstractMonitor implements ConnectionStateListener
{
  private static final String METRIC_IS_CONNECTED = "zk/connected";
  private static final String METRIC_RECONNECT_TIME = "zk/reconnect/time";
  private static final int NIL = -1;

  private final ServiceEmitter emitter;

  /**
   * Current connection state.
   */
  @GuardedBy("this")
  private ConnectionState currentState;

  /**
   * Time given by {@link System#currentTimeMillis()} at last disconnect.
   */
  @GuardedBy("this")
  private long lastDisconnectTime = NIL;

  public DruidConnectionStateListener(final ServiceEmitter emitter)
  {
    this.emitter = emitter;
  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework, ConnectionState newState)
  {
    if (newState.isConnected()) {
      final long disconnectDuration;

      synchronized (this) {
        if (lastDisconnectTime != NIL) {
          disconnectDuration = Math.max(0, System.currentTimeMillis() - lastDisconnectTime);
        } else {
          disconnectDuration = NIL;
        }

        currentState = newState;
        lastDisconnectTime = NIL;
      }

      if (disconnectDuration != NIL) {
        emitter.emit(ServiceMetricEvent.builder().setMetric(METRIC_RECONNECT_TIME, disconnectDuration));
      }
    } else {
      synchronized (this) {
        currentState = newState;
        lastDisconnectTime = Math.max(lastDisconnectTime, System.currentTimeMillis());
      }

      emitter.emit(AlertBuilder.create("ZooKeeper connection[%s]", newState));
    }
  }

  public boolean isConnected()
  {
    synchronized (this) {
      return currentState != null && currentState.isConnected();
    }
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emitter.emit(ServiceMetricEvent.builder().setMetric(METRIC_IS_CONNECTED, isConnected() ? 1 : 0));
    return true;
  }
}
