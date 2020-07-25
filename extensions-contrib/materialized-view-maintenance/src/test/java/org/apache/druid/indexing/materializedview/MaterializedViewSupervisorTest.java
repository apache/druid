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

package org.apache.druid.indexing.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexer.HadoopIOConfig;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexer.HadoopTuningConfig;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.HadoopIndexTask;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class MaterializedViewSupervisorTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private MetadataSupervisorManager metadataSupervisorManager;
  private SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private TaskQueue taskQueue;
  private MaterializedViewSupervisor supervisor;
  private String derivativeDatasourceName;
  private final ObjectMapper objectMapper = TestHelper.makeJsonMapper();

  @Before
  public void setUp()
  {
    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentTable();
    taskStorage = EasyMock.createMock(TaskStorage.class);
    taskMaster = EasyMock.createMock(TaskMaster.class);
    indexerMetadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector
    );
    metadataSupervisorManager = EasyMock.createMock(MetadataSupervisorManager.class);
    sqlSegmentsMetadataManager = EasyMock.createMock(SqlSegmentsMetadataManager.class);
    taskQueue = EasyMock.createMock(TaskQueue.class);
    taskQueue.start();
    objectMapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    MaterializedViewSupervisorSpec spec = new MaterializedViewSupervisorSpec(
        "base",
        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("dim")), null, null),
        new AggregatorFactory[]{new LongSumAggregatorFactory("m1", "m1")},
        HadoopTuningConfig.makeDefaultTuningConfig(),
        null,
        null,
        null,
        null,
        null,
        false,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        indexerMetadataStorageCoordinator,
        new MaterializedViewTaskConfig(),
        EasyMock.createMock(AuthorizerMapper.class),
        EasyMock.createMock(ChatHandlerProvider.class),
        new SupervisorStateManagerConfig()
    );
    derivativeDatasourceName = spec.getDataSourceName();
    supervisor = (MaterializedViewSupervisor) spec.createSupervisor();
  }

  @Test
  public void testCheckSegments() throws IOException
  {
    Set<DataSegment> baseSegments = Sets.newHashSet(
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
            "2015-01-04",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null),
            9,
            1024
        )
    );
    Set<DataSegment> derivativeSegments = Sets.newHashSet(
        new DataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null),
            9,
            1024
        ),
        new DataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "3015-01-01",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null),
            9,
            1024
        )
    );
    indexerMetadataStorageCoordinator.announceHistoricalSegments(baseSegments);
    indexerMetadataStorageCoordinator.announceHistoricalSegments(derivativeSegments);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    Pair<SortedMap<Interval, String>, Map<Interval, List<DataSegment>>> toBuildInterval = supervisor.checkSegments();
    Set<Interval> expectedToBuildInterval = Sets.newHashSet(Intervals.of("2015-01-01T00Z/2015-01-02T00Z"));
    Map<Interval, List<DataSegment>> expectedSegments = new HashMap<>();
    expectedSegments.put(
        Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
        Collections.singletonList(
            new DataSegment(
                "base",
                Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
                "2015-01-02",
                ImmutableMap.of(),
                ImmutableList.of("dim1", "dim2"),
                ImmutableList.of("m1"),
                new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null),
                9,
                1024
            )
        )
    );
    expectedSegments.put(
        Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
        Collections.singletonList(
            new DataSegment(
                "base",
                Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
                "2015-01-03",
                ImmutableMap.of(),
                ImmutableList.of("dim1", "dim2"),
                ImmutableList.of("m1"),
                new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null),
                9,
                1024
            )
        )
    );
    Assert.assertEquals(expectedToBuildInterval, toBuildInterval.lhs.keySet());
    Assert.assertEquals(expectedSegments, toBuildInterval.rhs);
  }

  @Test
  public void testCheckSegmentsAndSubmitTasks() throws IOException
  {
    Set<DataSegment> baseSegments = Sets.newHashSet(
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null),
            9,
            1024
        )
    );
    indexerMetadataStorageCoordinator.announceHistoricalSegments(baseSegments);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskStorage.getStatus("test_task1"))
            .andReturn(Optional.of(TaskStatus.failure("test_task1")))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("test_task2"))
            .andReturn(Optional.of(TaskStatus.running("test_task2")))
            .anyTimes();
    EasyMock.replay(taskStorage);

    Pair<Map<Interval, HadoopIndexTask>, Map<Interval, String>> runningTasksPair = supervisor.getRunningTasks();
    Map<Interval, HadoopIndexTask> runningTasks = runningTasksPair.lhs;
    Map<Interval, String> runningVersion = runningTasksPair.rhs;

    DataSchema dataSchema = new DataSchema(
        "test_datasource",
        null,
        null,
        null,
        TransformSpec.NONE,
        objectMapper
    );
    HadoopIOConfig hadoopIOConfig = new HadoopIOConfig(new HashMap<>(), null, null);
    HadoopIngestionSpec spec = new HadoopIngestionSpec(dataSchema, hadoopIOConfig, null);
    HadoopIndexTask task1 = new HadoopIndexTask(
        "test_task1",
        spec,
        null,
        null,
        null,
        objectMapper,
        null,
        null,
        null
    );
    runningTasks.put(Intervals.of("2015-01-01T00Z/2015-01-02T00Z"), task1);
    runningVersion.put(Intervals.of("2015-01-01T00Z/2015-01-02T00Z"), "test_version1");

    HadoopIndexTask task2 = new HadoopIndexTask(
        "test_task2",
        spec,
        null,
        null,
        null,
        objectMapper,
        null,
        null,
        null
    );
    runningTasks.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), task2);
    runningVersion.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), "test_version2");

    supervisor.checkSegmentsAndSubmitTasks();

    Map<Interval, HadoopIndexTask> expectedRunningTasks = new HashMap<>();
    Map<Interval, String> expectedRunningVersion = new HashMap<>();
    expectedRunningTasks.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), task2);
    expectedRunningVersion.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), "test_version2");

    Assert.assertEquals(expectedRunningTasks, runningTasks);
    Assert.assertEquals(expectedRunningVersion, runningVersion);

  }

  @Test
  public void testSuspendedDoesntRun()
  {
    MaterializedViewSupervisorSpec suspended = new MaterializedViewSupervisorSpec(
        "base",
        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("dim")), null, null),
        new AggregatorFactory[]{new LongSumAggregatorFactory("m1", "m1")},
        HadoopTuningConfig.makeDefaultTuningConfig(),
        null,
        null,
        null,
        null,
        null,
        true,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        indexerMetadataStorageCoordinator,
        new MaterializedViewTaskConfig(),
        EasyMock.createMock(AuthorizerMapper.class),
        EasyMock.createMock(ChatHandlerProvider.class),
        new SupervisorStateManagerConfig()
    );
    MaterializedViewSupervisor supervisor = (MaterializedViewSupervisor) suspended.createSupervisor();

    // mock IndexerSQLMetadataStorageCoordinator to ensure that retrieveDataSourceMetadata is not called
    // which will be true if truly suspended, since this is the first operation of the 'run' method otherwise
    IndexerSQLMetadataStorageCoordinator mock = EasyMock.createMock(IndexerSQLMetadataStorageCoordinator.class);
    EasyMock.expect(mock.retrieveDataSourceMetadata(suspended.getDataSourceName()))
            .andAnswer(() -> {
              Assert.fail();
              return null;
            })
            .anyTimes();

    EasyMock.replay(mock);
    supervisor.run();
  }
}
