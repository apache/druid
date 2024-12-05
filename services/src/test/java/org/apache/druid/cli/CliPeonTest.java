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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
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
  public void testCliPeonHeartbeatDimensions() throws IOException
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
    Assert.assertEquals(
        ImmutableMap.of(
            DruidMetrics.TASK_ID, taskId,
            DruidMetrics.GROUP_ID, groupId,
            DruidMetrics.DATASOURCE, datasource,
            DruidMetrics.TASK_TYPE, TestStreamingTask.TYPE,
            DruidMetrics.STATUS, TestStreamingTask.STATUS
        ),
        CliPeon.heartbeatDimensions(new TestStreamingTask(taskId, datasource, ImmutableMap.of(DruidMetrics.TAGS, ImmutableMap.of()), groupId))
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
        CliPeon.heartbeatDimensions(new TestStreamingTask(taskId, datasource, ImmutableMap.of(DruidMetrics.TAGS, tags), groupId))
    );
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
        String datasource,
        @Nullable Map context,
        @Nullable String groupId
    )
    {
      this(
          id,
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
        @Nullable TaskResource taskResource,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig,
        SeekableStreamIndexTaskIOConfig ioConfig,
        @Nullable Map context,
        @Nullable String groupId
    )
    {

      super(id, taskResource, dataSchema, tuningConfig, ioConfig, context, groupId);
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
