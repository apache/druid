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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.indexer.granularity.ArbitraryGranularitySpec;
import org.apache.druid.indexer.report.TaskReportFileWriter;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.CompactionIntervalSpec;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.worker.executor.ExecutorLifecycle;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.ChatHandlerProvider;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.metrics.LoadSpecHolder;
import org.apache.druid.server.metrics.ServiceStatusMonitor;
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
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.easymock.EasyMock.mock;

public class CliPeonTest
{

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testCliPeonK8sMode() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    GuiceRunnable runnable = new FakeCliPeon(file.getParent(), "k8s");
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }

  @Test
  public void testCliPeonNonK8sMode() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    GuiceRunnable runnable = new FakeCliPeon(file.getParent(), "httpRemote");
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }

  @Test
  public void testCliPeonNonK8sMode3() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    GuiceRunnable runnable = new FakeCliPeon(file.getParent(), "httpRemote");
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }

  @Test
  public void testCliPeonK8sANdWorkerIsK8sMode() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    GuiceRunnable runnable = new FakeCliPeon(file.getParent(), "k8sAndWorker");
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }

  @Test
  public void testCliPeonPolicyEnforcerInToolbox() throws IOException
  {
    CliPeon runnable = new CliPeon();
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    runnable.taskAndStatusFile = ImmutableList.of(file.getParent(), "1");

    Properties properties = new Properties();
    properties.setProperty("druid.policy.enforcer.type", "restrictAllTables");
    runnable.configure(properties);
    runnable.configure(properties, GuiceInjectors.makeStartupInjector());

    Injector secondaryInjector = runnable.makeInjector();
    Assert.assertEquals(new RestrictAllTablesPolicyEnforcer(null), secondaryInjector.getInstance(PolicyEnforcer.class));
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
  public void testCliPeonAllBindingsUp() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);

    CliPeon runnable = new CliPeon();
    runnable.taskAndStatusFile = ImmutableList.of(file.getParent(), "1");
    Properties properties = new Properties();
    runnable.configure(properties);
    runnable.configure(properties, GuiceInjectors.makeStartupInjector());
    Injector injector = runnable.makeInjector();

    Assert.assertNotNull(injector.getInstance(ExecutorLifecycle.class));
    Assert.assertNotNull(injector.getInstance(TaskRunner.class));
    Assert.assertNotNull(injector.getInstance(QuerySegmentWalker.class));
    Assert.assertNotNull(injector.getInstance(AppenderatorsManager.class));
    Assert.assertNotNull(injector.getInstance(ServerTypeConfig.class));
    Assert.assertNotNull(injector.getInstance(RowIngestionMetersFactory.class));
    Assert.assertNotNull(injector.getInstance(ChatHandlerProvider.class));
    Assert.assertNotNull(injector.getInstance(IntermediaryDataManager.class));
    Assert.assertNotNull(injector.getInstance(ShuffleClient.class));
    Assert.assertNotNull(injector.getInstance(SegmentHandoffNotifierFactory.class));
    Assert.assertNotNull(injector.getInstance(TaskReportFileWriter.class));


    Supplier<Map<String, Object>> hb = injector.getInstance(
        Key.get(new TypeLiteral<Supplier<Map<String, Object>>>() {}, Names.named(ServiceStatusMonitor.HEARTBEAT_TAGS_BINDING))
    );
    Assert.assertNotNull(hb);
    Assert.assertNotNull(hb.get());
  }

  @Test
  public void testCliPeonLocalTmpStorage() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);

    CliPeon runnable = new CliPeon();
    runnable.taskAndStatusFile = ImmutableList.of(file.getParent(), "1");
    Properties properties = new Properties();
    runnable.configure(properties);
    runnable.configure(properties, GuiceInjectors.makeStartupInjector());
    Injector secondaryInjector = runnable.makeInjector();
    Assert.assertNotNull(secondaryInjector);

    LocalTmpStorageConfig localTmpStorageConfig = secondaryInjector.getInstance(LocalTmpStorageConfig.class);
    Assert.assertEquals(new File(file.getParent(), "/tmp").getAbsolutePath(), localTmpStorageConfig.getTmpDir().getAbsolutePath());
  }

  @Test
  public void testCliPeonAllBindingsUp2() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    Task noopTask = new NoopTask("noopId", null, "noopDs", 0, 0, Map.of());
    final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();

    OBJECT_MAPPER.writeValueAsString(noopTask);
    FileUtils.write(file, OBJECT_MAPPER.writeValueAsString(noopTask), StandardCharsets.UTF_8);

    CliPeon runnable = new CliPeon();
    runnable.taskAndStatusFile = ImmutableList.of(file.getParent(), "1");
    Properties properties = new Properties();
    runnable.configure(properties);
    runnable.configure(properties, GuiceInjectors.makeStartupInjector());
    Injector injector = runnable.makeInjector();

    LoadSpecHolder instance = injector.getInstance(LoadSpecHolder.class);
    Assert.assertEquals(LookupLoadingSpec.ALL, instance.getLookupLoadingSpec());
    Assert.assertEquals(BroadcastDatasourceLoadingSpec.ALL, instance.getBroadcastDatasourceLoadingSpec());

    TaskHolder instance1 = injector.getInstance(TaskHolder.class);
    Assert.assertEquals("noopDs", instance1.getDataSource());
    Assert.assertEquals("noopId", instance1.getTaskId());

    Supplier<Map<String, Object>> hb = injector.getInstance(
        Key.get(new TypeLiteral<Supplier<Map<String, Object>>>() {}, Names.named(ServiceStatusMonitor.HEARTBEAT_TAGS_BINDING))
    );
    Assert.assertNotNull(hb);
    Assert.assertNotNull(hb.get());
  }


@Test
public void testCompactTaskWithTuningConfig() throws IOException
{
  File file = temporaryFolder.newFile("task.json");
  String s = "{"
             + "\"type\":\"compact\","
             + "\"id\":\"taskid\","
             + "\"dataSource\":\"test_ds\","
             + "\"ioConfig\":{"
             + "  \"type\":\"compact\","
             + "  \"inputSpec\":{"
             + "    \"type\":\"interval\","
             + "    \"interval\":\"2020-01-01/2020-01-02\""
             + "  },"
             + "  \"allowNonAlignedInterval\":false,"
             + "  \"dropExisting\":true"
             + "},"
             + "\"transformSpec\":{"
             + "  \"filter\":{"
             + "    \"type\":\"selector\","
             + "    \"dimension\":\"dim1\","
             + "    \"value\":\"b\""
             + "  }"
             + "},"
             + "\"tuningConfig\":{"
             + "  \"type\":\"compaction\","
             + "  \"maxRowsInMemory\":1000"
             + "}"
             + "}";
  FileUtils.write(file, s, StandardCharsets.UTF_8);

  CliPeon runnable = new CliPeon();
  runnable.taskAndStatusFile = ImmutableList.of(file.getParent(), "1");
  Properties properties = new Properties();
  runnable.configure(properties);
  runnable.configure(properties, GuiceInjectors.makeStartupInjector());
  Injector injector = runnable.makeInjector();

  Assert.assertNotNull(injector);
  LoadSpecHolder loadSpecHolder = injector.getInstance(LoadSpecHolder.class);
  Assert.assertTrue(loadSpecHolder instanceof NewTaskLoadSpecHolder);
  Assert.assertEquals(LookupLoadingSpec.ALL, loadSpecHolder.getLookupLoadingSpec());
  Assert.assertEquals(BroadcastDatasourceLoadingSpec.ALL, loadSpecHolder.getBroadcastDatasourceLoadingSpec());
  TaskHolder taskHolder = injector.getInstance(TaskHolder.class);
  Assert.assertTrue(taskHolder instanceof NewTaskPropertiesHolder);
  Assert.assertEquals("taskid", taskHolder.getTaskId());
  Assert.assertEquals("test_ds", taskHolder.getDataSource());
}

  @Test
  public void testCliPeonLookupAndExpressionModulesDoNotCycle() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);

    CliPeon runnable = new CliPeon();
    runnable.taskAndStatusFile = ImmutableList.of(file.getParent(), "1");
    Properties properties = new Properties();
    runnable.configure(properties);
    runnable.configure(properties, GuiceInjectors.makeStartupInjector());
    Injector injector = runnable.makeInjector();

    // Also ensure LoadSpecHolder is accessible and returns non-null specs.
    LoadSpecHolder loadSpecHolder = injector.getInstance(LoadSpecHolder.class);
    Assert.assertNotNull(loadSpecHolder.getLookupLoadingSpec());
    Assert.assertNotNull(loadSpecHolder.getBroadcastDatasourceLoadingSpec());
  }

  @Test
  public void testCliPeonLookupAndExpressionModulesDoNotCycleDefaultsFinal() throws IOException
  {
    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    File file = temporaryFolder.newFile("task.json");

    final CompactionTask.Builder builder = new CompactionTask.Builder(
        "test_ds",
        new SegmentCacheManagerFactory(TestIndex.INDEX_IO, objectMapper)
    );
    final CompactionTask compactionTask = builder
        .id("compact_test_taskid")
        .inputSpec(new CompactionIntervalSpec(Intervals.of("2020/2021"), null))
        .build();

    String s1 = objectMapper.writeValueAsString(compactionTask);
    FileUtils.write(file, s1, StandardCharsets.UTF_8);

    CliPeon runnable = new CliPeon();
    runnable.taskAndStatusFile = ImmutableList.of(file.getParent(), "1");
    Properties properties = new Properties();
    runnable.configure(properties);
    runnable.configure(properties, GuiceInjectors.makeStartupInjector());
    Injector injector = runnable.makeInjector();

    // Also ensure LoadSpecHolder is accessible and returns non-null specs.
    LoadSpecHolder loadSpecHolder = injector.getInstance(LoadSpecHolder.class);
    Assert.assertTrue(loadSpecHolder instanceof NewTaskLoadSpecHolder);
    Assert.assertEquals(LookupLoadingSpec.NONE, loadSpecHolder.getLookupLoadingSpec());
    Assert.assertEquals(BroadcastDatasourceLoadingSpec.ALL, loadSpecHolder.getBroadcastDatasourceLoadingSpec());

    TaskHolder taskHolder = injector.getInstance(TaskHolder.class);
    Assert.assertTrue(taskHolder instanceof NewTaskPropertiesHolder);
    Assert.assertEquals("compact_test_taskid", taskHolder.getTaskId());
    Assert.assertEquals("test_ds", taskHolder.getDataSource());
  }

  @Test
  public void testCliPeonLookupAndExpressionModulesDoNotCycleContextOverrides() throws IOException
  {
    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    File file = temporaryFolder.newFile("task.json");

    final CompactionTask.Builder builder = new CompactionTask.Builder(
        "test_ds",
        new SegmentCacheManagerFactory(TestIndex.INDEX_IO, objectMapper)
    );
    final CompactionTask compactionTask = builder
        .id("compact_test_taskid")
        .inputSpec(new CompactionIntervalSpec(Intervals.of("2020/2021"), null))
        .context(Map.of(
            "lookupLoadingMode", LookupLoadingSpec.Mode.NONE,
            "broadcastDatasourceLoadingMode", BroadcastDatasourceLoadingSpec.Mode.NONE
        ))
        .build();

    String s1 = objectMapper.writeValueAsString(compactionTask);
    FileUtils.write(file, s1, StandardCharsets.UTF_8);

    CliPeon runnable = new CliPeon();
    runnable.taskAndStatusFile = ImmutableList.of(file.getParent(), "1");
    Properties properties = new Properties();
    runnable.configure(properties);
    runnable.configure(properties, GuiceInjectors.makeStartupInjector());
    Injector injector = runnable.makeInjector();

    // Also ensure LoadSpecHolder is accessible and returns non-null specs.
    LoadSpecHolder loadSpecHolder = injector.getInstance(LoadSpecHolder.class);
    Assert.assertTrue(loadSpecHolder instanceof NewTaskLoadSpecHolder);
    Assert.assertEquals(LookupLoadingSpec.NONE, loadSpecHolder.getLookupLoadingSpec());
    Assert.assertEquals(BroadcastDatasourceLoadingSpec.NONE, loadSpecHolder.getBroadcastDatasourceLoadingSpec());

    TaskHolder taskHolder = injector.getInstance(TaskHolder.class);
    Assert.assertTrue(taskHolder instanceof NewTaskPropertiesHolder);
    Assert.assertEquals("compact_test_taskid", taskHolder.getTaskId());
    Assert.assertEquals("test_ds", taskHolder.getDataSource());
  }

  @Test
  public void testCliPeonLookupAndExpressionModulesDoNotCycleContextOverrides3() throws IOException
  {
    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    File file = temporaryFolder.newFile("task.json");

    final CompactionTask.Builder builder = new CompactionTask.Builder(
        "test_ds",
        new SegmentCacheManagerFactory(TestIndex.INDEX_IO, objectMapper)
    );
    final CompactionTask compactionTask = builder
        .id("compact_test_taskid")
        .inputSpec(new CompactionIntervalSpec(Intervals.of("2020/2021"), null))
        .transformSpec(new CompactionTransformSpec(new SelectorDimFilter("dim1", "foo", null)))
        .context(Map.of(
            "lookupLoadingMode", LookupLoadingSpec.Mode.NONE,
            "broadcastDatasourceLoadingMode", BroadcastDatasourceLoadingSpec.Mode.NONE
        ))
        .build();

    String s1 = objectMapper.writeValueAsString(compactionTask);
    FileUtils.write(file, s1, StandardCharsets.UTF_8);

    CliPeon runnable = new CliPeon();
    runnable.taskAndStatusFile = ImmutableList.of(file.getParent(), "1");
    Properties properties = new Properties();
    runnable.configure(properties);
    runnable.configure(properties, GuiceInjectors.makeStartupInjector());
//    Injector injector1 = GuiceInjectors.makeStartupInjectorWithModules(List.of(new LookupModule()));
//    runnable.configure(properties, injector);
    Injector injector = runnable.makeInjector();

//        Injector injectorfluff = Initialization.makeInjectorWithModules(
//        GuiceInjectors.makeStartupInjector(),
//        List.of()
//    );
//    Injector injectorfluff = GuiceInjectors.makeStartupInjector();
    runnable.configure(properties, injector);


    // Also ensure LoadSpecHolder is accessible and returns non-null specs.
    LoadSpecHolder loadSpecHolder = injector.getInstance(LoadSpecHolder.class);
    Assert.assertTrue(loadSpecHolder instanceof NewTaskLoadSpecHolder);
    Assert.assertEquals(LookupLoadingSpec.NONE, loadSpecHolder.getLookupLoadingSpec());
    Assert.assertEquals(BroadcastDatasourceLoadingSpec.NONE, loadSpecHolder.getBroadcastDatasourceLoadingSpec());

    TaskHolder taskHolder = injector.getInstance(TaskHolder.class);
    Assert.assertTrue(taskHolder instanceof NewTaskPropertiesHolder);
    Assert.assertEquals("compact_test_taskid", taskHolder.getTaskId());
    Assert.assertEquals("test_ds", taskHolder.getDataSource());
  }

  private static class FakeCliPeon extends CliPeon
  {
    List<String> taskAndStatusFile = new ArrayList<>();

    FakeCliPeon(String taskDirectory, String runnerType)
    {
      try {
        taskAndStatusFile.add(taskDirectory);
        taskAndStatusFile.add("1");

        Field privateField = CliPeon.class
            .getDeclaredField("taskAndStatusFile");
        privateField.setAccessible(true);
        privateField.set(this, taskAndStatusFile);

        System.setProperty("druid.indexer.runner.type", runnerType);
      }
      catch (Exception ex) {
        // do nothing
      }

    }
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
        @Nullable String groupId,
        DataSchema dataSchema
    )
    {
      this(
          id,
          supervisorId,
          null,
          dataSchema,
          mock(SeekableStreamIndexTaskTuningConfig.class),
          new TestSeekableStreamIndexTaskIOConfig(),
          context,
          groupId
      );
    }

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

    public TestStreamingTask(
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
