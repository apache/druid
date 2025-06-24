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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.discovery.DruidLeaderSelector;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDruidLeaderSelector implements DruidLeaderSelector
{
  private final AtomicInteger localTerm = new AtomicInteger(0);
  private final AtomicBoolean isLeader = new AtomicBoolean(false);
  private volatile Listener listener;

  public void becomeLeader()
  {
    if (isLeader.compareAndSet(false, true)) {
      if (listener != null) {
        listener.becomeLeader();
      }
      localTerm.incrementAndGet();
    }
  }

  public void stopBeingLeader()
  {
    if (isLeader.compareAndSet(true, false) && listener != null) {
      listener.stopBeingLeader();
    }
  }

  @Nullable
  @Override
  public String getCurrentLeader()
  {
    return "me";
  }

  @Override
  public boolean isLeader()
  {
    return isLeader.get();
  }

  @Override
  public int localTerm()
  {
    return localTerm.get();
  }

  @Override
  public void registerListener(Listener listener)
  {
    this.listener = listener;
    if (isLeader()) {
      listener.becomeLeader();
    }
  }

  @Override
  public void unregisterListener()
  {
    listener = null;
  }
}
