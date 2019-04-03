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
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.LocalTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.actions.TaskAuditLogConfig;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.util.Collections;
import java.util.Map;

public abstract class IngestionTestBase
{
  public static final String DATA_SOURCE = "test";

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final TestUtils testUtils = new TestUtils();
  private final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
  private TaskStorage taskStorage;
  private IndexerSQLMetadataStorageCoordinator storageCoordinator;
  private TaskLockbox lockbox;

  @Before
  public void setUp()
  {
    final SQLMetadataConnector connector = derbyConnectorRule.getConnector();
    connector.createTaskTables();
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
    storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnectorRule.getConnector()
    );
    lockbox = new TaskLockbox(taskStorage);
  }

  public LocalTaskActionClient createActionClient(Task task)
  {
    return new LocalTaskActionClient(task, taskStorage, createTaskActionToolbox(), new TaskAuditLogConfig(false));
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

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public TaskStorage getTaskStorage()
  {
    return taskStorage;
  }

  public TaskLockbox getLockbox()
  {
    return lockbox;
  }

  public IndexerSQLMetadataStorageCoordinator getStorageCoordinator()
  {
    return storageCoordinator;
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

  public IndexTask.IndexIngestionSpec createIngestionSpec(
      File baseDir,
      ParseSpec parseSpec,
      GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean appendToExisting
  )
  {
    return createIngestionSpec(baseDir, parseSpec, TransformSpec.NONE, granularitySpec, tuningConfig, appendToExisting);
  }

  public IndexTask.IndexIngestionSpec createIngestionSpec(
      File baseDir,
      ParseSpec parseSpec,
      TransformSpec transformSpec,
      GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean appendToExisting
  )
  {
    return new IndexTask.IndexIngestionSpec(
        new DataSchema(
            DATA_SOURCE,
            objectMapper.convertValue(
                new StringInputRowParser(parseSpec, null),
                Map.class
            ),
            new AggregatorFactory[]{
                new LongSumAggregatorFactory("val", "val")
            },
            granularitySpec != null ? granularitySpec : new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.MINUTE,
                Collections.singletonList(Intervals.of("2014/2015"))
            ),
            transformSpec,
            objectMapper
        ),
        new IndexTask.IndexIOConfig(
            new LocalFirehoseFactory(
                baseDir,
                "druid*",
                null
            ),
            appendToExisting
        ),
        tuningConfig
    );
  }
}
