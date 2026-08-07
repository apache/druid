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

package org.apache.druid.indexing.worker.shuffle;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecLookup;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Verifies that {@link LocalIntermediaryDataManager#addSegment} is safe under concurrent
 * invocation by multiple appender threads sharing the same supervisorTaskId.
 *
 * Without the thread-safe cursor introduced for issue #19443, this test would surface
 * either a {@link java.util.ConcurrentModificationException} on the location map or a
 * {@link java.util.NoSuchElementException} on the underlying iterator.
 */
public class LocalIntermediaryDataManagerConcurrencyTest
{
  private static final int LOCATION_COUNT = 4;
  private static final long LOCATION_CAPACITY_BYTES = 1_073_741_824L; // 1 GiB; ample so reservation never fails
  private static final int THREADS = 16;
  private static final int CALLS_PER_THREAD = 200;
  private static final String SUPERVISOR_TASK_ID = "supervisorTaskId";

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private LocalIntermediaryDataManager intermediaryDataManager;
  private File sharedSegmentDir;

  @Before
  public void setUp() throws IOException
  {
    final WorkerConfig workerConfig = new WorkerConfig();
    final ImmutableList.Builder<StorageLocationConfig> locations = ImmutableList.builder();
    for (int i = 0; i < LOCATION_COUNT; i++) {
      locations.add(new StorageLocationConfig(tempDir.newFolder("loc_" + i), LOCATION_CAPACITY_BYTES, null));
    }
    final TaskConfig taskConfig = new TaskConfigBuilder()
        .setShuffleDataLocations(locations.build())
        .build();
    final OverlordClient overlordClient = new NoopOverlordClient();
    intermediaryDataManager = new LocalIntermediaryDataManager(workerConfig, taskConfig, overlordClient);
    intermediaryDataManager.start();
    // Pre-built shared input dir keeps per-call work small so the race window dominates wall time.
    sharedSegmentDir = tempDir.newFolder("shared_input");
    FileUtils.write(new File(sharedSegmentDir, "data.txt"), "x", StandardCharsets.UTF_8);
    FileUtils.writeByteArrayToFile(new File(sharedSegmentDir, "version.bin"), Ints.toByteArray(9));
  }

  @After
  public void tearDown()
  {
    intermediaryDataManager.stop();
  }

  @Test(timeout = 90_000)
  public void testConcurrentAddSegmentSharedSupervisorIsThreadSafe() throws Exception
  {
    final Interval interval = Intervals.of("2018/2019");
    final ExecutorService executor = Execs.multiThreaded(THREADS, "local-intermediary-data-manager-concurrency-test-%d");
    final CountDownLatch startGate = new CountDownLatch(1);

    final List<Future<Void>> futures = new ArrayList<>(THREADS);
    for (int t = 0; t < THREADS; t++) {
      final int threadId = t;
      futures.add(executor.submit(() -> {
        startGate.await();
        for (int call = 0; call < CALLS_PER_THREAD; call++) {
          final String subTaskId = "thread_" + threadId + "_call_" + call;
          final DataSegment segment = newSegment(interval, threadId * CALLS_PER_THREAD + call);
          intermediaryDataManager.addSegment(SUPERVISOR_TASK_ID, subTaskId, segment, sharedSegmentDir);
        }
        return null;
      }));
    }

    startGate.countDown();

    // Collect results; any exception inside a worker (CME / NoSuchElementException)
    // surfaces here as ExecutionException and fails the test.
    for (Future<Void> f : futures) {
      f.get(60, TimeUnit.SECONDS);
    }
    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
  }

  private DataSegment newSegment(Interval interval, int bucketId)
  {
    return new DataSegment(
        "dataSource",
        interval,
        "version",
        null,
        null,
        null,
        new TestShardSpec(bucketId),
        9,
        10
    );
  }

  private static class TestShardSpec implements BucketNumberedShardSpec<BuildingShardSpec<ShardSpec>>
  {
    private final int bucketId;

    private TestShardSpec(int bucketId)
    {
      this.bucketId = bucketId;
    }

    @Override
    public int getBucketId()
    {
      return bucketId;
    }

    @Override
    public BuildingShardSpec<ShardSpec> convert(int partitionId)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ShardSpecLookup getLookup(List<? extends ShardSpec> shardSpecs)
    {
      throw new UnsupportedOperationException();
    }
  }
}
