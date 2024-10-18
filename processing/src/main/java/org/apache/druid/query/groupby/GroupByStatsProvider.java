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

package org.apache.druid.query.groupby;

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.query.groupby.epinephelinae.LimitedTemporaryStorage;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects stats for group by queries like used merged buffer count, spilled bytes and group by resource acquisition time.
 */
@LazySingleton
public class GroupByStatsProvider
{
  private final AtomicLong resourceAcquisitionTimeNs = new AtomicLong(0);
  private final AtomicLong resourceAcquisitionCount = new AtomicLong(0);

  private final ConcurrentLinkedQueue<LimitedTemporaryStorage> temporaryStorages;

  @Inject
  public GroupByStatsProvider()
  {
    this.temporaryStorages = new ConcurrentLinkedQueue<>();
  }

  public synchronized void groupByResourceAcquisitionTimeNs(long delayNs)
  {
    resourceAcquisitionTimeNs.addAndGet(delayNs);
    resourceAcquisitionCount.incrementAndGet();
  }

  public synchronized long getAndResetGroupByResourceAcquisitionStats()
  {
    long average = resourceAcquisitionCount.get() != 0 ?
                   (resourceAcquisitionTimeNs.get() / resourceAcquisitionCount.get()) : 0;

    resourceAcquisitionTimeNs.set(0);
    resourceAcquisitionCount.set(0);

    return average;
  }

  public void registerTemporaryStorage(LimitedTemporaryStorage temporaryStorage)
  {
    temporaryStorages.add(temporaryStorage);
  }

  public long getSpilledBytes()
  {
    long spilledBytes = 0;

    Iterator<LimitedTemporaryStorage> iterator = temporaryStorages.iterator();

    while (iterator.hasNext()) {
      LimitedTemporaryStorage limitedTemporaryStorage = iterator.next();

      spilledBytes += limitedTemporaryStorage.currentSize();

      if (limitedTemporaryStorage.isClosed()) {
        iterator.remove();
      }
    }

    return spilledBytes;
  }
}
