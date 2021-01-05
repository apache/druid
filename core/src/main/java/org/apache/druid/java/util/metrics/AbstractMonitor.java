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


import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.util.concurrent.Future;


/**
 */
public abstract class AbstractMonitor implements Monitor
{
  private volatile boolean started = false;
  
  private volatile Future<?> scheduledFuture;

  @Override
  public void start()
  {
    started = true;
  }

  @Override
  public void stop()
  {
    started = false;
  }

  @Override
  public boolean monitor(ServiceEmitter emitter)
  {
    if (started) {
      return doMonitor(emitter);
    }

    return false;
  }

  public abstract boolean doMonitor(ServiceEmitter emitter);

  @Override
  public Future<?> getScheduledFuture()
  {
    return scheduledFuture;
  }

  @Override
  public void setScheduledFuture(Future<?> scheduledFuture)
  {
    this.scheduledFuture = scheduledFuture;
  }
}
