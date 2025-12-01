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

package org.apache.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexer.granularity.ArbitraryGranularitySpec;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.CompactionIntervalSpec;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.metrics.LoadSpecHolder;
import org.apache.druid.server.metrics.TaskHolder;
import org.apache.druid.storage.local.LocalTmpStorageConfig;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.easymock.EasyMock.mock;

public class CliPeonTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  private final CompactionTask.Builder compactBuilder =
      new CompactionTask.Builder("test_ds", new SegmentCacheManagerFactory(TestIndex.INDEX_IO, mapper))
      .id("compact_test_taskid")
      .inputSpec(new CompactionIntervalSpec(Intervals.of("2020/2021"), null));

  @Test
  public void testCliPeonK8sMode() throws IOException
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.indexer.runner.type", "k8s");
    final Injector peonInjector = makePeonInjector(properties, NoopTask.create());
    final ExecutorLifecycleConfig executorLifecycleConfig = peonInjector.getInstance(ExecutorLifecycleConfig.class);
    Assert.assertFalse(executorLifecycleConfig.isParentStreamDefined());
  }

  @Test
  public void testCliPeonNonK8sMode() throws IOException
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.indexer.runner.type", "httpRemote");
    final Injector peonInjector = makePeonInjector(properties, NoopTask.create());
    final ExecutorLifecycleConfig executorLifecycleConfig = peonInjector.getInstance(ExecutorLifecycleConfig.class);
    Assert.assertTrue(executorLifecycleConfig.isParentStreamDefined());
  }

  @Test
  public void testCliPeonK8sANdWorkerIsK8sMode() throws IOException
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.indexer.runner.type", "k8sAndWorker");
    final Injector peonInjector = makePeonInjector(properties, NoopTask.create());
    final ExecutorLifecycleConfig executorLifecycleConfig = peonInjector.getInstance(ExecutorLifecycleConfig.class);
    Assert.assertFalse(executorLifecycleConfig.isParentStreamDefined());
  }

  @Test
  public void testCliPeonPolicyEnforcerInToolbox() throws IOException
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.policy.enforcer.type", "restrictAllTables");
    final Injector peonInjector = makePeonInjector(properties, NoopTask.create());
    Assert.assertEquals(
        new RestrictAllTablesPolicyEnforcer(null),
        peonInjector.getInstance(PolicyEnforcer.class)
    );
  }

  @Test
  public void testCliPeonHeartbeatDimensions()
  {
    // non-streaming task
    String taskId = "testTaskId";
    String groupId = "testGroupId";
    String datasource = "testDatasource";
    Map<String, String> tags = ImmutableMap.of("tag1", "value1");
    Assert.assertEquals(
        ImmutableMap.of(
            DruidMetrics.TASK_ID, taskId,
            DruidMetrics.GROUP_ID, groupId,
            DruidMetrics.DATASOURCE, datasource,
            DruidMetrics.TASK_TYPE, NoopTask.TYPE
        ),
        CliPeon.heartbeatDimensions(new TestTask(taskId, groupId, datasource, 0, 0, ImmutableMap.of()))
    );

    // streaming task with empty ags
    String supervisor = "testSupervisor";
    Assert.assertEquals(
        ImmutableMap.of(
            DruidMetrics.TASK_ID, taskId,
            DruidMetrics.GROUP_ID, groupId,
            DruidMetrics.DATASOURCE, datasource,
            DruidMetrics.TASK_TYPE, TestStreamingTask.TYPE,
            DruidMetrics.STATUS, TestStreamingTask.STATUS
        ),
        CliPeon.heartbeatDimensions(new TestStreamingTask(taskId, supervisor, datasource, ImmutableMap.of(DruidMetrics.TAGS, ImmutableMap.of()), groupId))
    );

    // streaming task with non-empty ags
    Assert.assertEquals(
        ImmutableMap.of(
            DruidMetrics.TASK_ID, taskId,
            DruidMetrics.GROUP_ID, groupId,
            DruidMetrics.DATASOURCE, datasource,
            DruidMetrics.TASK_TYPE, TestStreamingTask.TYPE,
            DruidMetrics.STATUS, TestStreamingTask.STATUS,
            DruidMetrics.TAGS, tags
        ),
        CliPeon.heartbeatDimensions(new TestStreamingTask(taskId, supervisor, datasource, ImmutableMap.of(DruidMetrics.TAGS, tags), groupId))
    );
  }

  @Test
  public void testCliPeonLocalTmpStorage() throws IOException
  {
    final Properties properties = new Properties();
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, mapper.writeValueAsString(NoopTask.create()), StandardCharsets.UTF_8);
    final Injector peonInjector = makePeonInjector(file, properties);

    LocalTmpStorageConfig localTmpStorageConfig = peonInjector.getInstance(LocalTmpStorageConfig.class);
    Assert.assertEquals(new File(file.getParent(), "/tmp").getAbsolutePath(), localTmpStorageConfig.getTmpDir().getAbsolutePath());
  }

  @Test
  public void testTaskWithDefaultContext() throws IOException
  {
    final CompactionTask compactionTask = compactBuilder.build();
    final Injector peonInjector = makePeonInjector(new Properties(), compactionTask);

    verifyLoadSpecHolder(peonInjector.getInstance(LoadSpecHolder.class), compactionTask);
    verifyTaskHolder(peonInjector.getInstance(TaskHolder.class), compactionTask);
  }

  @Test
  public void testTaskWithContextOverrides() throws IOException
  {
    final CompactionTask compactionTask = compactBuilder
        .context(Map.of(
            "lookupLoadingMode", LookupLoadingSpec.Mode.NONE,
            "broadcastDatasourceLoadingMode", BroadcastDatasourceLoadingSpec.Mode.NONE
        ))
        .build();

    final Injector peonInjector = makePeonInjector(new Properties(), compactionTask);

    verifyLoadSpecHolder(peonInjector.getInstance(LoadSpecHolder.class), compactionTask);
    verifyTaskHolder(peonInjector.getInstance(TaskHolder.class), compactionTask);
  }

  @Test
  public void testTaskWithMetricsSpecDoesNotCauseCyclicDependency() throws IOException
  {
    final CompactionTask compactionTask = compactBuilder
        .metricsSpec(new AggregatorFactory[]{
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("val", "val")
        })
        .build();

    final Injector peonInjector = makePeonInjector(new Properties(), compactionTask);

    verifyLoadSpecHolder(peonInjector.getInstance(LoadSpecHolder.class), compactionTask);
    verifyTaskHolder(peonInjector.getInstance(TaskHolder.class), compactionTask);
  }

  @Test
  public void testTaskWithMonitorsAndMetricsSpecDoNotCauseCyclicDependency() throws IOException
  {
    final CompactionTask compactionTask = compactBuilder
        .metricsSpec(new AggregatorFactory[]{
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("val", "val")
        }).build();

    final Properties properties = new Properties();
    properties.setProperty(
        "druid.monitoring.monitors",
        "[\"org.apache.druid.server.metrics.ServiceStatusMonitor\","
        + " \"org.apache.druid.java.util.metrics.JvmMonitor\"]"
    );
    final Injector peonInjector = makePeonInjector(properties, compactionTask);

    verifyLoadSpecHolder(peonInjector.getInstance(LoadSpecHolder.class), compactionTask);
    verifyTaskHolder(peonInjector.getInstance(TaskHolder.class), compactionTask);
  }

  private Injector makePeonInjector(File taskFile, Properties properties)
  {
    final CliPeon peon = new CliPeon();
    peon.taskAndStatusFile = ImmutableList.of(taskFile.getParent(), "1");

    peon.configure(properties);
    peon.configure(properties, GuiceInjectors.makeStartupInjector());
    return peon.makeInjector(Set.of(NodeRole.PEON));
  }

  private Injector makePeonInjector(Properties properties, Task task) throws IOException
  {
    File taskFile = temporaryFolder.newFile("task.json");
    FileUtils.write(taskFile, mapper.writeValueAsString(task), StandardCharsets.UTF_8);
    return makePeonInjector(taskFile, properties);
  }

  private static void verifyLoadSpecHolder(LoadSpecHolder observedLoadSpecHolder, Task task)
  {
    Assert.assertTrue(observedLoadSpecHolder instanceof PeonLoadSpecHolder);
    Assert.assertEquals(task.getLookupLoadingSpec(), observedLoadSpecHolder.getLookupLoadingSpec());
    Assert.assertEquals(
        task.getBroadcastDatasourceLoadingSpec(),
        observedLoadSpecHolder.getBroadcastDatasourceLoadingSpec()
    );
  }

  private static void verifyTaskHolder(TaskHolder observedTaskHolder, Task task)
  {
    Assert.assertTrue(observedTaskHolder instanceof PeonTaskHolder);
    Assert.assertEquals(task.getId(), observedTaskHolder.getTaskId());
    Assert.assertEquals(task.getDataSource(), observedTaskHolder.getDataSource());
  }

  private static class TestTask extends NoopTask
  {

    public TestTask(
        String id,
        String groupId,
        String dataSource,
        long runTimeMillis,
        long isReadyTime,
        Map<String, Object> context
    )
    {
      super(id, groupId, dataSource, runTimeMillis, isReadyTime, context);
    }
  }

  private static class TestStreamingTask extends SeekableStreamIndexTask<String, String, ByteEntity>
  {
    static final String TYPE = "testStreaming";
    static final String STATUS = SeekableStreamIndexTaskRunner.Status.PAUSED.toString();

    public TestStreamingTask(
        String id,
        @Nullable String supervisorId,
        String datasource,
        @Nullable Map context,
        @Nullable String groupId
    )
    {
      this(
          id,
          supervisorId,
          null,
          DataSchema.builder()
              .withDataSource(datasource)
              .withTimestamp(new TimestampSpec(null, null, null))
              .withDimensions(new DimensionsSpec(Collections.emptyList()))
              .withGranularity(new ArbitraryGranularitySpec(new AllGranularity(), Collections.emptyList()))
              .build(),
          mock(SeekableStreamIndexTaskTuningConfig.class),
          new TestSeekableStreamIndexTaskIOConfig(),
          context,
          groupId
      );
    }

    private TestStreamingTask(
        String id,
        @Nullable String supervisorId,
        @Nullable TaskResource taskResource,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig,
        SeekableStreamIndexTaskIOConfig ioConfig,
        @Nullable Map context,
        @Nullable String groupId
    )
    {

      super(id, supervisorId, taskResource, dataSchema, tuningConfig, ioConfig, context, groupId);
    }

    @Override
    protected SeekableStreamIndexTaskRunner<String, String, ByteEntity> createTaskRunner()
    {
      return null;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> newTaskRecordSupplier(final TaskToolbox toolbox)
    {
      return null;
    }

    @Override
    public String getCurrentRunnerStatus()
    {
      return STATUS;
    }

    @Override
    public String getType()
    {
      return TYPE;
    }
  }

  private static class TestSeekableStreamIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<String, String>
  {
    public TestSeekableStreamIndexTaskIOConfig()
    {
      super(
          null,
          "someSequence",
          new SeekableStreamStartSequenceNumbers<>("abc", "def", Collections.emptyMap(), Collections.emptyMap(), null),
          new SeekableStreamEndSequenceNumbers<>("abc", "def", Collections.emptyMap(), Collections.emptyMap()),
          false,
          DateTimes.nowUtc().minusDays(2),
          DateTimes.nowUtc(),
          new CsvInputFormat(null, null, true, null, 0, null),
          Duration.standardHours(2).getStandardMinutes()
      );
    }
  }
}
