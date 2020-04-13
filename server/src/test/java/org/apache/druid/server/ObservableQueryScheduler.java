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

import io.github.resilience4j.bulkhead.Bulkhead;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.server.initialization.ServerConfig;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link QueryScheduler} for testing, with counters on its internal functions so its operation can be observed
 * and verified by tests
 */
public class ObservableQueryScheduler extends QueryScheduler
{
  private final AtomicLong totalAcquired;
  private final AtomicLong totalReleased;
  private final AtomicLong laneAcquired;
  private final AtomicLong laneNotAcquired;
  private final AtomicLong laneReleased;
  private final AtomicLong totalPrioritizedAndLaned;
  private final AtomicLong totalRun;

  public ObservableQueryScheduler(
      int totalNumThreads,
      QueryPrioritizationStrategy prioritizationStrategy,
      QueryLaningStrategy laningStrategy,
      ServerConfig serverConfig
  )
  {
    super(totalNumThreads, prioritizationStrategy, laningStrategy, serverConfig);

    totalAcquired = new AtomicLong();
    totalReleased = new AtomicLong();
    laneAcquired = new AtomicLong();
    laneNotAcquired = new AtomicLong();
    laneReleased = new AtomicLong();
    totalPrioritizedAndLaned = new AtomicLong();
    totalRun = new AtomicLong();
  }

  @Override
  public <T> Sequence<T> run(
      Query<?> query,
      Sequence<T> resultSequence
  )
  {
    return super.run(query, resultSequence).withBaggage(totalRun::incrementAndGet);
  }

  @Override
  public <T> Query<T> prioritizeAndLaneQuery(
      QueryPlus<T> queryPlus,
      Set<SegmentServerSelector> segments
  )
  {
    totalPrioritizedAndLaned.incrementAndGet();
    return super.prioritizeAndLaneQuery(queryPlus, segments);
  }

  @Override
  List<Bulkhead> acquireLanes(Query<?> query)
  {
    List<Bulkhead> bulkheads = super.acquireLanes(query);
    if (bulkheads.stream().anyMatch(b -> b.getName().equals(QueryScheduler.TOTAL))) {
      totalAcquired.incrementAndGet();
    }
    if (bulkheads.stream().anyMatch(b -> !b.getName().equals(QueryScheduler.TOTAL))) {
      laneAcquired.incrementAndGet();
    }

    return bulkheads;
  }

  @Override
  void releaseLanes(List<Bulkhead> bulkheads)
  {
    super.releaseLanes(bulkheads);
    if (bulkheads.stream().anyMatch(b -> b.getName().equals(QueryScheduler.TOTAL))) {
      totalReleased.incrementAndGet();
    }
    if (bulkheads.stream().anyMatch(b -> !b.getName().equals(QueryScheduler.TOTAL))) {
      laneReleased.incrementAndGet();
      if (bulkheads.size() == 1) {
        laneNotAcquired.incrementAndGet();
      }
    }
  }

  @Override
  void finishLanes(List<Bulkhead> bulkheads)
  {
    super.finishLanes(bulkheads);
    if (bulkheads.stream().anyMatch(b -> b.getName().equals(QueryScheduler.TOTAL))) {
      totalReleased.incrementAndGet();
    }
    if (bulkheads.stream().anyMatch(b -> !b.getName().equals(QueryScheduler.TOTAL))) {
      laneReleased.incrementAndGet();
    }
  }

  /**
   * Number of times that 'total' query count semaphore was acquired
   */
  public AtomicLong getTotalAcquired()
  {
    return totalAcquired;
  }

  /**
   * Number of times that 'total' query count semaphore was released
   */
  public AtomicLong getTotalReleased()
  {
    return totalReleased;
  }

  /**
   * Number of times that the query count semaphore of any lane was acquired
   */
  public AtomicLong getLaneAcquired()
  {
    return laneAcquired;
  }

  /**
   * Number of times that the query count semaphore of any lane was acquired but the 'total' semaphore was NOT acquired
   */
  public AtomicLong getLaneNotAcquired()
  {
    return laneNotAcquired;
  }

  /**
   * Number of times that the query count semaphore of any lane was released
   */
  public AtomicLong getLaneReleased()
  {
    return laneReleased;
  }

  /**
   * Number of times that {@link QueryScheduler#prioritizeAndLaneQuery} was called
   */
  public AtomicLong getTotalPrioritizedAndLaned()
  {
    return totalPrioritizedAndLaned;
  }

  /**
   * Number of times that {@link QueryScheduler#run} was called
   */
  public AtomicLong getTotalRun()
  {
    return totalRun;
  }
}
