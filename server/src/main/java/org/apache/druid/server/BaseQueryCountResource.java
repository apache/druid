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

package org.apache.druid.server;

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.server.metrics.QueryCountStatsProvider;

import java.util.concurrent.atomic.AtomicLong;

@LazySingleton
public class BaseQueryCountResource implements QueryCountStatsProvider
{
  private final AtomicLong successfulQueryCount = new AtomicLong();
  private final AtomicLong failedQueryCount = new AtomicLong();
  private final AtomicLong interruptedQueryCount = new AtomicLong();
  private final AtomicLong timedOutQueryCount = new AtomicLong();

  @Override
  public long getSuccessfulQueryCount()
  {
    return successfulQueryCount.get();
  }

  @Override
  public long getFailedQueryCount()
  {
    return failedQueryCount.get();
  }

  @Override
  public long getInterruptedQueryCount()
  {
    return interruptedQueryCount.get();
  }

  @Override
  public long getTimedOutQueryCount()
  {
    return timedOutQueryCount.get();
  }

  @Override
  public void incrementSuccess()
  {
    successfulQueryCount.incrementAndGet();
  }

  @Override
  public void incrementFailed()
  {
    failedQueryCount.incrementAndGet();
  }

  @Override
  public void incrementInterrupted()
  {
    interruptedQueryCount.incrementAndGet();
  }

  @Override
  public void incrementTimedOut()
  {
    timedOutQueryCount.incrementAndGet();
  }
}
