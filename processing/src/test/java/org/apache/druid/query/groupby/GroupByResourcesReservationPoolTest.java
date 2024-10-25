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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryResourceId;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class GroupByResourcesReservationPoolTest
{

  /**
   * CONFIG + QUERY require exactly 1 merge buffer to succeed if 'willMergeRunners' is true while allocating the resources
   */
  private static final GroupByQueryConfig CONFIG = new GroupByQueryConfig();
  private static final GroupByQuery QUERY = GroupByQuery.builder()
                                                        .setInterval(Intervals.ETERNITY)
                                                        .setDataSource("foo")
                                                        .setDimensions(
                                                            ImmutableList.of(
                                                                new DefaultDimensionSpec("dim2", "_d0")
                                                            )
                                                        )
                                                        .setGranularity(Granularities.ALL)
                                                        .setContext(
                                                            ImmutableMap.of("timeout", 0)
                                                        ) // Query can block indefinitely
                                                        .build();

  /**
   * This test confirms that the interleaved GroupByResourcesReservationPool.reserve() and GroupByResourcesReservationPool.clean()
   * between multiple threads succeed. It is specifically designed to test the case when the operations are interleaved in the
   * following manner:
   * <p>
   * THREAD1                      THREAD2
   * pool.reserve(query1)
   *                              pool.reserve(query2)
   * pool.clean(query1)
   * <p>
   * This test assumes a few things about the implementation of the interfaces, which are laid out in the comments.
   * <p>
   * The test should complete under 10 seconds, and the majority of the time would be consumed by waiting for the thread
   * that sleeps for 5 seconds
   */
  @Ignore(
      "Isn't run as a part of CI since it sleeps for 5 seconds. Callers must run the test manually if any changes are made "
      + "to the corresponding class"
  )
  @Test(timeout = 100_000L)
  public void testInterleavedReserveAndRemove()
  {
    ExecutorService executor = Execs.multiThreaded(3, "group-by-resources-reservation-pool-test-%d");

    // Sanity checks that the query will acquire exactly one merge buffer. This safeguards the test being useful in
    // case the merge buffer acquisition code changes to acquire less than one merge buffer (the test would be
    // useless in that case) or more than one merge buffer (the test would incorrectly fail in that case)
    Assert.assertEquals(
        1,
        GroupByQueryResources.countRequiredMergeBufferNumForMergingQueryRunner(CONFIG, QUERY)
        + GroupByQueryResources.countRequiredMergeBufferNumForToolchestMerge(QUERY)
    );

    // Blocking pool with a single buffer, which means only one of the queries can succeed at a time
    BlockingPool<ByteBuffer> mergeBufferPool = new DefaultBlockingPool<>(() -> ByteBuffer.allocate(100), 1);

    final GroupByResourcesReservationPool groupByResourcesReservationPool =
        new GroupByResourcesReservationPool(mergeBufferPool, CONFIG);

    // Latch indicating that the first thread has called reservationPool.reserve()
    CountDownLatch reserveCalledByFirstThread = new CountDownLatch(1);
    // Latch indicating that the second thread has called reservationPool.reserve()
    CountDownLatch reserveCalledBySecondThread = new CountDownLatch(1);
    // Latch indicating that all the threads have been completed successfully. Main thread waits on this latch before exiting
    CountDownLatch threadsCompleted = new CountDownLatch(2);

    // THREAD 1
    executor.submit(() -> {

      QueryResourceId queryResourceId1 = new QueryResourceId("test-id-1")
      {
        @Override
        public int hashCode()
        {
          // IMPORTANT ASSUMPTION: For the test to be useful, it assumes that under the hood we are using a
          // ConcurrentHashMap<QueryResourceId, GroupByResources> (or a concurrent map with similar implementation) that
          // implements granular locking of the nodes
          // The hashCode of the queryResourceId used in Thread1 and Thread2 is the same. Therefore, both the queryIds
          // would be guarded by the same lock
          return 10;
        }

        @Override
        public boolean equals(Object o)
        {
          return super.equals(o);
        }
      };
      groupByResourcesReservationPool.reserve(
          queryResourceId1,
          QUERY,
          true,
          new GroupByStatsProvider.PerQueryStats()
      );
      reserveCalledByFirstThread.countDown();
      try {
        reserveCalledBySecondThread.await();
      }
      catch (InterruptedException e) {
        Assert.fail("Interrupted while waiting for second reserve call to be made");
      }
      groupByResourcesReservationPool.clean(queryResourceId1);
      threadsCompleted.countDown();
    });

    // THREAD 2
    executor.submit(() -> {
      try {
        reserveCalledByFirstThread.await();
      }
      catch (InterruptedException e) {
        Assert.fail("Interrupted while waiting for first reserve call to be made");
      }

      QueryResourceId queryResourceId2 = new QueryResourceId("test-id-2")
      {
        @Override
        public int hashCode()
        {
          return 10;
        }

        @Override
        public boolean equals(Object o)
        {
          return super.equals(o);
        }
      };

      // Since the reserve() call is blocking, we need to execute it separately, so that we can count down the latch
      // and inform Thread1 the reserve call has been made by this thread
      executor.submit(() -> {
        groupByResourcesReservationPool.reserve(
            queryResourceId2,
            QUERY,
            true,
            new GroupByStatsProvider.PerQueryStats()
        );
        threadsCompleted.countDown();
      });
      try {
        // This sleep call "ensures" that the statment pool.reserve(queryResourceId2) is called before we release the
        // latch (that will cause Thread1 to release the acquired resources). It still doesn't guarantee the previous
        // statement, however that's the best we can do, given that reserve() is blocking
        Thread.sleep(5_000);
      }
      catch (InterruptedException e) {
        Assert.fail("Interrupted while sleeping");
      }
      reserveCalledBySecondThread.countDown();
    });

    try {
      threadsCompleted.await();
    }
    catch (InterruptedException e) {
      Assert.fail("Interrupted while waiting for the threads to complete");
    }
  }

  @Test
  public void testMultipleSimultaneousAllocationAttemptsFail()
  {
    BlockingPool<ByteBuffer> mergeBufferPool = new DefaultBlockingPool<>(() -> ByteBuffer.allocate(100), 1);
    final GroupByResourcesReservationPool groupByResourcesReservationPool =
        new GroupByResourcesReservationPool(mergeBufferPool, CONFIG);
    QueryResourceId queryResourceId = new QueryResourceId("test-id");

    groupByResourcesReservationPool.reserve(
        queryResourceId,
        QUERY,
        true,
        new GroupByStatsProvider.PerQueryStats()
    );

    Assert.assertThrows(
        DruidException.class,
        () -> groupByResourcesReservationPool.reserve(
            queryResourceId,
            QUERY,
            true,
            new GroupByStatsProvider.PerQueryStats()
        )
    );
  }

  @Test
  public void testMultipleSequentialAllocationAttemptsSucceed()
  {
    BlockingPool<ByteBuffer> mergeBufferPool = new DefaultBlockingPool<>(() -> ByteBuffer.allocate(100), 1);
    final GroupByResourcesReservationPool groupByResourcesReservationPool =
        new GroupByResourcesReservationPool(mergeBufferPool, CONFIG);
    QueryResourceId queryResourceId = new QueryResourceId("test-id");

    groupByResourcesReservationPool.reserve(
        queryResourceId,
        QUERY,
        true,
        new GroupByStatsProvider.PerQueryStats()
    );
    GroupByQueryResources oldResources = groupByResourcesReservationPool.fetch(queryResourceId);

    // Cleanup the resources
    groupByResourcesReservationPool.clean(queryResourceId);

    // Repeat the calls
    groupByResourcesReservationPool.reserve(
        queryResourceId,
        QUERY,
        true,
        new GroupByStatsProvider.PerQueryStats()
    );
    GroupByQueryResources newResources = groupByResourcesReservationPool.fetch(queryResourceId);
    Assert.assertNotNull(newResources);

    Assert.assertNotSame(oldResources, newResources);
  }
}
