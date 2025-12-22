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

package org.apache.druid.benchmark;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.BrokerViewOfCoordinatorConfig;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordination.TestCoordinatorClient;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark for {@link BrokerServerView} to test lock contention between:
 * - Query path operations (getQueryRunner)
 * - Segment callback operations (serverAddedSegment/serverRemovedSegment)
 *
 * This benchmark is designed to identify performance issues with the giant lock
 * being acquired on every query and every segment add/drop callback.
 *
 * Run with:
 * java -jar benchmarks/target/benchmarks.jar BrokerServerViewBenchmark
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UseG1GC", "-Xms5g", "-Xmx5g"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BrokerServerViewBenchmark
{
  private static final String DATA_SOURCE = "benchmark_datasource";
  private static final String DEFAULT_TIER = "default_tier";

  @Param({"1000"})
  private int numServers;

  @Param({"1000"})
  private int numSegmentsPerServer;

  private BrokerServerView brokerServerView;
  private BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig;
  private TestFilteredServerInventoryView baseView;
  private List<DruidServer> servers;
  private AtomicLong segmentCounter;

  @Setup(Level.Trial)
  public void setup()
  {
    brokerViewOfCoordinatorConfig = new BrokerViewOfCoordinatorConfig(new TestCoordinatorClient());
    brokerViewOfCoordinatorConfig.start();

    baseView = new TestFilteredServerInventoryView();
    segmentCounter = new AtomicLong(0);

    // Create the BrokerServerView
    brokerServerView = new BrokerServerView(
        server -> new QueryableDruidServer(server, new NoOpQueryRunner()),
        baseView,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        new NoopServiceEmitter(),
        new BrokerSegmentWatcherConfig(),
        brokerViewOfCoordinatorConfig
    );

    // Setup servers - synchronous mode is ON by default, so all callbacks execute immediately
    ImmutableList.Builder<DruidServer> serverBuilder = ImmutableList.builder();
    for (int i = 0; i < numServers; i++) {
      DruidServer server = createServer(i);
      serverBuilder.add(server);
      baseView.addServer(server);

      // Trigger server added callback (synchronous - runs immediately)
      baseView.triggerServerAdded(server);

      // Add segments to server
      for (int j = 0; j < numSegmentsPerServer; j++) {
        DataSegment segment = createSegment(i, j);
        server.addDataSegment(segment);
        baseView.triggerSegmentAdded(server.getMetadata(), segment);
      }
    }
    servers = serverBuilder.build();

    // Signal initialization complete
    baseView.triggerSegmentViewInitialized();

    // Verify all servers are registered - fail fast if setup didn't work
    for (DruidServer server : servers) {
      QueryRunner<?> runner = brokerServerView.getQueryRunner(server);
      if (runner == null) {
        throw new IllegalStateException(
            "Server " + server.getName() + " not registered in BrokerServerView after setup!"
        );
      }
    }

    // Switch to async mode for benchmark - segment callbacks during benchmark
    // will be dispatched to the executor, simulating real contention
    baseView.setSynchronousMode(false);
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    // TODO
  }

  /**
   * Benchmark getQueryRunner() in isolation - no contention.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void getQueryRunnerNoContention(Blackhole blackhole)
  {
    // Pick a server using safe modulo (avoid negative index on overflow)
    int serverIdx = (int) ((segmentCounter.incrementAndGet() & Long.MAX_VALUE) % numServers);
    DruidServer server = servers.get(serverIdx);
    QueryRunner<?> runner = brokerServerView.getQueryRunner(server);
    blackhole.consume(runner);
  }

  /**
   * Benchmark getQueryRunner() with concurrent segment additions.
   * This simulates the real-world scenario where queries are happening
   * while segments are being added/removed.
   */
  @Benchmark
  @Group("contention")
  @GroupThreads(4)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void getQueryRunnerWithContention(Blackhole blackhole, ContentionState state)
  {
    int serverIdx = (int) ((state.queryCounter.incrementAndGet() & Long.MAX_VALUE) % numServers);
    DruidServer server = servers.get(serverIdx);
    QueryRunner<?> runner = brokerServerView.getQueryRunner(server);
    blackhole.consume(runner);
  }

  @Benchmark
  @Group("contention")
  @GroupThreads(2)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void segmentAddRemoveWithContention(Blackhole blackhole, ContentionState state)
  {
    long counter = state.segmentCounter.incrementAndGet() & Long.MAX_VALUE;
    int serverIdx = (int) (counter % numServers);
    DruidServer server = servers.get(serverIdx);

    // Alternate between add and remove
    if ((counter & 1) == 0) {
      DataSegment segment = createSegment(serverIdx, (int) ((counter / numServers) % Integer.MAX_VALUE) + numSegmentsPerServer);
      baseView.triggerSegmentAdded(server.getMetadata(), segment);
      blackhole.consume(segment);
    } else {
      DataSegment segment = createSegment(serverIdx, (int) ((counter / numServers) % numSegmentsPerServer));
      baseView.triggerSegmentRemoved(server.getMetadata(), segment);
      blackhole.consume(segment);
    }
  }

  /**
   * Heavy query contention scenario - many query threads, few segment threads.
   */
  @Benchmark
  @Group("heavyQuery")
  @GroupThreads(8)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void heavyQueryLoad(Blackhole blackhole, ContentionState state)
  {
    int serverIdx = (int) ((state.queryCounter.incrementAndGet() & Long.MAX_VALUE) % numServers);
    DruidServer server = servers.get(serverIdx);
    QueryRunner<?> runner = brokerServerView.getQueryRunner(server);
    blackhole.consume(runner);
  }

  @Benchmark
  @Group("heavyQuery")
  @GroupThreads(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void lightSegmentLoad(Blackhole blackhole, ContentionState state)
  {
    long counter = state.segmentCounter.incrementAndGet() & Long.MAX_VALUE;
    int serverIdx = (int) (counter % numServers);
    DruidServer server = servers.get(serverIdx);

    DataSegment segment = createSegment(serverIdx, (int) ((counter / numServers) % Integer.MAX_VALUE) + numSegmentsPerServer * 2);
    baseView.triggerSegmentAdded(server.getMetadata(), segment);
    blackhole.consume(segment);
  }

  /**
   * Heavy segment churn scenario - few query threads, many segment threads.
   */
  @Benchmark
  @Group("heavySegment")
  @GroupThreads(2)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void lightQueryLoad(Blackhole blackhole, ContentionState state)
  {
    int serverIdx = (int) ((state.queryCounter.incrementAndGet() & Long.MAX_VALUE) % numServers);
    DruidServer server = servers.get(serverIdx);
    QueryRunner<?> runner = brokerServerView.getQueryRunner(server);
    blackhole.consume(runner);
  }

  @Benchmark
  @Group("heavySegment")
  @GroupThreads(6)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void heavySegmentChurn(Blackhole blackhole, ContentionState state)
  {
    long counter = state.segmentCounter.incrementAndGet() & Long.MAX_VALUE;
    int serverIdx = (int) (counter % numServers);
    DruidServer server = servers.get(serverIdx);

    // Alternate between add and remove
    if ((counter & 1) == 0) {
      DataSegment segment = createSegment(serverIdx, (int) ((counter / numServers) % Integer.MAX_VALUE) + numSegmentsPerServer * 3);
      baseView.triggerSegmentAdded(server.getMetadata(), segment);
      blackhole.consume(segment);
    } else {
      DataSegment segment = createSegment(serverIdx, (int) ((counter / numServers) % numSegmentsPerServer));
      baseView.triggerSegmentRemoved(server.getMetadata(), segment);
      blackhole.consume(segment);
    }
  }

  @State(Scope.Group)
  public static class ContentionState
  {
    final AtomicLong queryCounter = new AtomicLong(0);
    final AtomicLong segmentCounter = new AtomicLong(0);
  }

  private DruidServer createServer(int index)
  {
    return new DruidServer(
        "server_" + index,
        "127.0.0." + index + ":8080",
        null,
        Long.MAX_VALUE,
        ServerType.HISTORICAL,
        DEFAULT_TIER,
        0
    );
  }

  private DataSegment createSegment(int serverIndex, int segmentIndex)
  {
    return DataSegment.builder()
                      .dataSource(DATA_SOURCE)
                      .interval(Intervals.of(
                          String.format("2020-01-%02d/2020-01-%02d", (segmentIndex % 28) + 1, (segmentIndex % 28) + 2)
                      ))
                      .version("v" + serverIndex + "_" + segmentIndex)
                      .shardSpec(NoneShardSpec.instance())
                      .loadSpec(ImmutableMap.of("type", "local", "path", "somewhere"))
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .binaryVersion(9)
                      .size(0)
                      .build();
  }

  /**
   * A no-op QueryRunner for benchmark purposes.
   */
  private static class NoOpQueryRunner implements QueryRunner<Object>
  {
    @Override
    public Sequence<Object> run(QueryPlus<Object> queryPlus, ResponseContext responseContext)
    {
      return Sequences.empty();
    }
  }

  /**
   * Test implementation of FilteredServerInventoryView that allows manual triggering
   * of callbacks for benchmark control.
   */
  private static class TestFilteredServerInventoryView implements FilteredServerInventoryView
  {
    private final ConcurrentMap<String, DruidServer> servers = new ConcurrentHashMap<>();
    private volatile ServerView.SegmentCallback segmentCallback;
    private volatile ServerView.ServerCallback serverCallback;
    private volatile Executor callbackExecutor;

    // When true, execute callbacks synchronously (for setup phase)
    private volatile boolean synchronousMode = true;

    void addServer(DruidServer server)
    {
      servers.put(server.getName(), server);
    }

    void setSynchronousMode(boolean synchronous)
    {
      this.synchronousMode = synchronous;
    }

    void triggerServerAdded(DruidServer server)
    {
      if (serverCallback != null) {
        Runnable task = () -> serverCallback.serverAdded(server);
        if (synchronousMode) {
          task.run();
        } else if (callbackExecutor != null) {
          callbackExecutor.execute(task);
        }
      }
    }

    void triggerSegmentAdded(DruidServerMetadata server, DataSegment segment)
    {
      if (segmentCallback != null) {
        Runnable task = () -> segmentCallback.segmentAdded(server, segment);
        if (synchronousMode) {
          task.run();
        } else if (callbackExecutor != null) {
          callbackExecutor.execute(task);
        }
      }
    }

    void triggerSegmentRemoved(DruidServerMetadata server, DataSegment segment)
    {
      if (segmentCallback != null) {
        Runnable task = () -> segmentCallback.segmentRemoved(server, segment);
        if (synchronousMode) {
          task.run();
        } else if (callbackExecutor != null) {
          callbackExecutor.execute(task);
        }
      }
    }

    void triggerSegmentViewInitialized()
    {
      if (segmentCallback != null) {
        Runnable task = () -> segmentCallback.segmentViewInitialized();
        if (synchronousMode) {
          task.run();
        } else if (callbackExecutor != null) {
          callbackExecutor.execute(task);
        }
      }
    }

    @Override
    public void registerSegmentCallback(
        Executor exec,
        ServerView.SegmentCallback callback,
        Predicate<Pair<DruidServerMetadata, DataSegment>> filter
    )
    {
      this.callbackExecutor = exec;
      this.segmentCallback = new ServerView.SegmentCallback()
      {
        @Override
        public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
        {
          if (filter == null || filter.apply(Pair.of(server, segment))) {
            return callback.segmentAdded(server, segment);
          }
          return ServerView.CallbackAction.CONTINUE;
        }

        @Override
        public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
        {
          if (filter == null || filter.apply(Pair.of(server, segment))) {
            return callback.segmentRemoved(server, segment);
          }
          return ServerView.CallbackAction.CONTINUE;
        }

        @Override
        public ServerView.CallbackAction segmentViewInitialized()
        {
          return callback.segmentViewInitialized();
        }

        @Override
        public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
        {
          return callback.segmentSchemasAnnounced(segmentSchemas);
        }
      };
    }

    @Override
    public void registerServerCallback(Executor exec, ServerView.ServerCallback callback)
    {
      this.callbackExecutor = exec;
      this.serverCallback = callback;
    }

    @Nullable
    @Override
    public DruidServer getInventoryValue(String serverKey)
    {
      return servers.get(serverKey);
    }

    @Override
    public Collection<DruidServer> getInventory()
    {
      return servers.values();
    }

    @Override
    public boolean isStarted()
    {
      return true;
    }

    @Override
    public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
    {
      DruidServer server = servers.get(serverKey);
      return server != null && server.getSegment(segment.getId()) != null;
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(BrokerServerViewBenchmark.class.getSimpleName())
        .forks(1)
        .jvmArgsAppend("-XX:+UseG1GC", "-Xms10g", "-Xmx10g")
        .warmupIterations(3)
        .warmupTime(TimeValue.seconds(1))
        .measurementIterations(10)
        .measurementTime(TimeValue.seconds(2))
        .build();
    new Runner(opt).run();
  }
}

