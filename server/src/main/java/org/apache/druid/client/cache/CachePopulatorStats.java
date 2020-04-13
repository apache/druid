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

package org.apache.druid.client.cache;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread safe collector of {@link CachePopulator} statistics, utilized {@link CacheMonitor} to emit cache metrics.
 * Like the {@link CachePopulator}, this is used as a singleton.
 *
 * See {@link org.apache.druid.guice.DruidProcessingModule#getCachePopulator} which supplies either
 * {@link ForegroundCachePopulator} or {@link BackgroundCachePopulator}, as configured, for more details.
 */
public class CachePopulatorStats
{
  private final AtomicLong okCounter = new AtomicLong();
  private final AtomicLong errorCounter = new AtomicLong();
  private final AtomicLong oversizedCounter = new AtomicLong();

  public void incrementOk()
  {
    okCounter.incrementAndGet();
  }

  public void incrementError()
  {
    errorCounter.incrementAndGet();
  }

  public void incrementOversized()
  {
    oversizedCounter.incrementAndGet();
  }

  public Snapshot snapshot()
  {
    return new Snapshot(
        okCounter.get(),
        errorCounter.get(),
        oversizedCounter.get()
    );
  }

  public static class Snapshot
  {
    private final long numOk;
    private final long numError;
    private final long numOversized;

    Snapshot(final long numOk, final long numError, final long numOversized)
    {
      this.numOk = numOk;
      this.numError = numError;
      this.numOversized = numOversized;
    }

    public long getNumOk()
    {
      return numOk;
    }

    public long getNumError()
    {
      return numError;
    }

    public long getNumOversized()
    {
      return numOversized;
    }

    public Snapshot delta(Snapshot oldSnapshot)
    {
      if (oldSnapshot == null) {
        return this;
      } else {
        return new Snapshot(
            numOk - oldSnapshot.numOk,
            numError - oldSnapshot.numError,
            numOversized - oldSnapshot.numOversized
        );
      }
    }
  }
}
