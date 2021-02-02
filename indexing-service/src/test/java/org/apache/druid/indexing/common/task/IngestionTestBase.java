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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.SegmentInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.NoopDataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

public abstract class IngestionTestBase extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final TestUtils testUtils = new TestUtils();
  private final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
  private SegmentLoaderFactory segmentLoaderFactory;
  private TaskStorage taskStorage;
  private IndexerSQLMetadataStorageCoordinator storageCoordinator;
  private SegmentsMetadataManager segmentsMetadataManager;
  private TaskLockbox lockbox;

  @Before
  public void setUpIngestionTestBase() throws IOException
  {
    temporaryFolder.create();

    final SQLMetadataConnector connector = derbyConnectorRule.getConnector();
    connector.createTaskTables();
    connector.createSegmentTable();
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
    storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnectorRule.getConnector()
    );
    segmentsMetadataManager = new SqlSegmentsMetadataManager(
        objectMapper,
        SegmentsMetadataManagerConfig::new,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        derbyConnectorRule.getConnector()
    );
    lockbox = new TaskLockbox(taskStorage, storageCoordinator);
    segmentLoaderFactory = new SegmentLoaderFactory(getIndexIO(), getObjectMapper());
  }

  @After
  public void tearDownIngestionTestBase()
  {
    temporaryFolder.delete();
  }

  public TestLocalTaskActionClientFactory createActionClientFactory()
  {
    return new TestLocalTaskActionClientFactory();
  }

  public TestLocalTaskActionClient createActionClient(Task task)
  {
    return new TestLocalTaskActionClient(task);
  }

  public void prepareTaskForLocking(Task task) throws EntryExistsException
  {
    lockbox.add(task);
    taskStorage.insert(task, TaskStatus.running(task.getId()));
  }

  public void shutdownTask(Task task)
  {
    lockbox.remove(task);
  }

  public SegmentLoader newSegmentLoader(File storageDir)
  {
    return segmentLoaderFactory.manufacturate(storageDir);
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public TaskStorage getTaskStorage()
  {
    return taskStorage;
  }

  public SegmentLoaderFactory getSegmentLoaderFactory()
  {
    return segmentLoaderFactory;
  }

  public IndexerMetadataStorageCoordinator getMetadataStorageCoordinator()
  {
    return storageCoordinator;
  }

  public SegmentsMetadataManager getSegmentsMetadataManager()
  {
    return segmentsMetadataManager;
  }

  public TaskLockbox getLockbox()
  {
    return lockbox;
  }

  public IndexerSQLMetadataStorageCoordinator getStorageCoordinator()
  {
    return storageCoordinator;
  }

  public RowIngestionMetersFactory getRowIngestionMetersFactory()
  {
    return testUtils.getRowIngestionMetersFactory();
  }

  public TaskActionToolbox createTaskActionToolbox()
  {
    storageCoordinator.start();
    return new TaskActionToolbox(
        lockbox,
        taskStorage,
        storageCoordinator,
        new NoopServiceEmitter(),
        null
    );
  }

  public IndexIO getIndexIO()
  {
    return testUtils.getTestIndexIO();
  }

  public IndexMergerV9 getIndexMerger()
  {
    return testUtils.getTestIndexMergerV9();
  }

  public class TestLocalTaskActionClientFactory implements TaskActionClientFactory
  {
    @Override
    public TaskActionClient create(Task task)
    {
      return new TestLocalTaskActionClient(task);
    }
  }

  public class TestLocalTaskActionClient extends CountingLocalTaskActionClientForTest
  {
    private final Set<DataSegment> publishedSegments = new HashSet<>();

    private TestLocalTaskActionClient(Task task)
    {
      super(task, taskStorage, createTaskActionToolbox());
    }

    @Override
    public <RetType> RetType submit(TaskAction<RetType> taskAction)
    {
      final RetType result = super.submit(taskAction);
      if (taskAction instanceof SegmentTransactionalInsertAction) {
        publishedSegments.addAll(((SegmentTransactionalInsertAction) taskAction).getSegments());
      } else if (taskAction instanceof SegmentInsertAction) {
        publishedSegments.addAll(((SegmentInsertAction) taskAction).getSegments());
      }
      return result;
    }

    public Set<DataSegment> getPublishedSegments()
    {
      return publishedSegments;
    }
  }

  public class TestTaskRunner implements TaskRunner
  {
    private TestLocalTaskActionClient taskActionClient;
    private File taskReportsFile;

    @Override
    public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void start()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerListener(TaskRunnerListener listener, Executor executor)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterListener(String listenerId)
    {
      throw new UnsupportedOperationException();
    }

    public TestLocalTaskActionClient getTaskActionClient()
    {
      return taskActionClient;
    }

    public File getTaskReportsFile()
    {
      return taskReportsFile;
    }

    public List<DataSegment> getPublishedSegments()
    {
      final List<DataSegment> segments = new ArrayList<>(taskActionClient.getPublishedSegments());
      Collections.sort(segments);
      return segments;
    }

    @Override
    public ListenableFuture<TaskStatus> run(Task task)
    {
      try {
        lockbox.add(task);
        taskStorage.insert(task, TaskStatus.running(task.getId()));
        taskActionClient = createActionClient(task);
        taskReportsFile = temporaryFolder.newFile(
            StringUtils.format("ingestionTestBase-%s.json", System.currentTimeMillis())
        );

        final TaskToolbox box = new TaskToolbox(
            null,
            new DruidNode("druid/middlemanager", "localhost", false, 8091, null, true, false),
            taskActionClient,
            null,
            new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig()),
            new NoopDataSegmentKiller(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            NoopJoinableFactory.INSTANCE,
            null,
            null,
            objectMapper,
            temporaryFolder.newFolder(),
            getIndexIO(),
            null,
            null,
            null,
            getIndexMerger(),
            null,
            null,
            null,
            null,
            new SingleFileTaskReportFileWriter(taskReportsFile),
            null,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            new NoopChatHandlerProvider(),
            testUtils.getRowIngestionMetersFactory(),
            new TestAppenderatorsManager(),
            new NoopIndexingServiceClient(),
            null,
            null,
            null
        );

        if (task.isReady(box.getTaskActionClient())) {
          return Futures.immediateFuture(task.run(box));
        } else {
          throw new ISE("task is not ready");
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        lockbox.remove(task);
      }
    }

    @Override
    public void shutdown(String taskid, String reason)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stop()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ScalingStats> getScalingStats()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getTotalTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getIdleTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getUsedTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLazyTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getBlacklistedTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }
  }
}
