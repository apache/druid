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

package org.apache.druid.query;

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.java.util.common.guava.Sequence;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


public class QueryScheduler
{
  // maybe instead use a fancy library?
  private final Semaphore totes;
  private final Map<String, Semaphore> lanes;
  private final QuerySchedulingStrategy strategy;
  private final Set<Query<?>> runningQueries;

  public QueryScheduler(
      int totalNumThreads,
      QuerySchedulingStrategy strategy
  )
  {
    this.strategy = strategy;
    this.totes = new Semaphore(totalNumThreads);
    this.lanes = new HashMap<>();

    for (Object2IntMap.Entry<String> entry : strategy.getLaneLimits().object2IntEntrySet()) {
      lanes.put(entry.getKey(), new Semaphore(entry.getIntValue()));
    }
    this.runningQueries = Sets.newConcurrentHashSet();
  }

  public <T> Query<T> schedule(QueryPlus<T> query, Set<SegmentDescriptor> descriptors)
  {
    try {
      if (totes.tryAcquire(0, TimeUnit.MILLISECONDS)) {
        Query<T> prioritizedAndLaned = strategy.prioritizeQuery(query, descriptors);
        String lane = prioritizedAndLaned.getContextValue("queryLane");
        if (lanes.containsKey(lane)) {
          if (lanes.get(lane).tryAcquire(0, TimeUnit.MILLISECONDS)) {
            runningQueries.add(prioritizedAndLaned);
            return prioritizedAndLaned;
          }
        } else {
          runningQueries.add(prioritizedAndLaned);
          return prioritizedAndLaned;
        }
      }
    } catch (InterruptedException ex) {
      throw new QueryCapacityExceededException();
    }
    throw new QueryCapacityExceededException();
  }

  public <T> Sequence<T> run(Query<?> query, Sequence<T> resultSequence)
  {
    return resultSequence.withBaggage(() -> complete(query));
  }

  public int getTotalAvailableCapacity()
  {
    return totes.availablePermits();
  }

  public int getLaneAvailableCapacity(String lane)
  {
    if (lanes.containsKey(lane)) {
      return lanes.get(lane).availablePermits();
    }
    return -1;
  }

  private void complete(Query<?> query)
  {
//    try {
//      Thread.sleep(10000);
//    }
//    catch (InterruptedException ie) {
//      // eat it
//    }

    if (runningQueries.remove(query)) {
      String lane = query.getContextValue("queryLane");
      if (lanes.containsKey(lane)) {
        lanes.get(lane).release();
      }
      totes.release();
    }
  }
}
