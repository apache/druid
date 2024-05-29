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
import junit.framework.AssertionFailedError;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.EntryAlreadyExists;
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
import org.apache.druid.java.util.common.ISE;
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
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class MaterializedViewSupervisorTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private MetadataSupervisorManager metadataSupervisorManager;
  private SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private TaskQueue taskQueue;
  private MaterializedViewSupervisor supervisor;
  private String derivativeDatasourceName;
  private MaterializedViewSupervisorSpec spec;
  private final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  private SegmentSchemaManager segmentSchemaManager;

  @Before
  public void setUp()
  {
    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentSchemasTable();
    derbyConnector.createSegmentTable();
    taskStorage = EasyMock.createMock(TaskStorage.class);
    taskMaster = EasyMock.createMock(TaskMaster.class);
    segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        objectMapper,
        derbyConnector
    );
    indexerMetadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create()
    );
    metadataSupervisorManager = EasyMock.createMock(MetadataSupervisorManager.class);
    sqlSegmentsMetadataManager = EasyMock.createMock(SqlSegmentsMetadataManager.class);
    taskQueue = EasyMock.createMock(TaskQueue.class);

    objectMapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    spec = new MaterializedViewSupervisorSpec(
        "base",
        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("dim"))),
        new AggregatorFactory[]{new LongSumAggregatorFactory("m1", "m1")},
        HadoopTuningConfig.makeDefaultTuningConfig(),
        null,
        null,
        null,
        null,
        Collections.singletonMap("maxTaskCount", 2),
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
    List<DataSegment> baseSegments = createBaseSegments();
    Set<DataSegment> derivativeSegments = Sets.newHashSet(createDerivativeSegments());

    final Interval day1 = baseSegments.get(0).getInterval();
    final Interval day2 = new Interval(day1.getStart().plusDays(1), day1.getEnd().plusDays(1));

    indexerMetadataStorageCoordinator.commitSegments(new HashSet<>(baseSegments), null);
    indexerMetadataStorageCoordinator.commitSegments(derivativeSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();

    Pair<SortedMap<Interval, String>, Map<Interval, List<DataSegment>>> toBuildInterval
        = supervisor.checkSegments();

    Map<Interval, List<DataSegment>> expectedSegments = ImmutableMap.of(
        day1, Collections.singletonList(baseSegments.get(0)),
        day2, Collections.singletonList(baseSegments.get(1))
    );
    Assert.assertEquals(Collections.singleton(day1), toBuildInterval.lhs.keySet());
    Assert.assertEquals(expectedSegments, toBuildInterval.rhs);
  }

  @Test
  public void testSubmitTasksDoesNotFailIfTaskAlreadyExists() throws IOException
  {
    Set<DataSegment> baseSegments = Sets.newHashSet(createBaseSegments());
    Set<DataSegment> derivativeSegments = Sets.newHashSet(createDerivativeSegments());

    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    indexerMetadataStorageCoordinator.commitSegments(derivativeSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();

    EasyMock.expect(taskQueue.add(EasyMock.anyObject()))
            .andThrow(EntryAlreadyExists.exception("Task ID already exists"));

    EasyMock.replay(taskMaster, taskStorage, taskQueue);

    supervisor.checkSegmentsAndSubmitTasks();

    EasyMock.verify(taskMaster, taskStorage, taskQueue);
  }

  @Test
  public void testSubmitTasksFailsIfTaskCannotBeAdded() throws IOException
  {
    Set<DataSegment> baseSegments = Sets.newHashSet(createBaseSegments());
    Set<DataSegment> derivativeSegments = Sets.newHashSet(createDerivativeSegments());

    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    indexerMetadataStorageCoordinator.commitSegments(derivativeSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();

    EasyMock.expect(taskQueue.add(EasyMock.anyObject()))
            .andThrow(new ISE("Could not add task"));

    EasyMock.replay(taskMaster, taskStorage, taskQueue);

    ISE exception = Assert.assertThrows(
        ISE.class,
        () -> supervisor.checkSegmentsAndSubmitTasks()
    );
    Assert.assertEquals("Could not add task", exception.getMessage());

    EasyMock.verify(taskMaster, taskStorage, taskQueue);
  }

  @Test
  public void testCheckSegmentsAndSubmitTasks() throws IOException
  {
    Set<DataSegment> baseSegments = Collections.singleton(createBaseSegments().get(0));
    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskStorage.getStatus("test_task1"))
            .andReturn(Optional.of(TaskStatus.failure("test_task1", "Dummy task status failure err message")))
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
  public void testCreateTaskSucceeds()
  {
    List<DataSegment> baseSegments = createBaseSegments().subList(0, 1);

    HadoopIndexTask task = spec.createTask(
        Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
        "2015-01-03",
        baseSegments
    );

    Assert.assertNotNull(task);
  }

  @Test
  public void testSuspendedDoesNotRun()
  {
    MaterializedViewSupervisorSpec suspended = new MaterializedViewSupervisorSpec(
        "base",
        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("dim"))),
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
            .andThrow(new AssertionFailedError())
            .anyTimes();

    EasyMock.replay(mock);
    supervisor.run();
  }

  @Test
  public void testResetOffsetsNotSupported()
  {
    MaterializedViewSupervisorSpec suspended = new MaterializedViewSupervisorSpec(
        "base",
        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("dim"))),
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
    Assert.assertThrows(
        "Reset offsets not supported in MaterializedViewSupervisor",
        UnsupportedOperationException.class,
        () -> supervisor.resetOffsets(null)
    );
  }

  private List<DataSegment> createBaseSegments()
  {
    return Arrays.asList(
        createSegment("base", "2015-01-01T00Z/2015-01-02T00Z", "2015-01-02"),
        createSegment("base", "2015-01-02T00Z/2015-01-03T00Z", "2015-01-03"),
        createSegment("base", "2015-01-03T00Z/2015-01-04T00Z", "2015-01-04")
    );
  }

  private List<DataSegment> createDerivativeSegments()
  {
    return Arrays.asList(
        createSegment(derivativeDatasourceName, "2015-01-01T00Z/2015-01-02T00Z", "2015-01-02"),
        createSegment(derivativeDatasourceName, "2015-01-02T00Z/2015-01-03T00Z", "3015-01-01")
    );
  }

  private DataSegment createSegment(String datasource, String interval, String version)
  {
    return new DataSegment(
        datasource,
        Intervals.of(interval),
        version,
        Collections.emptyMap(),
        Arrays.asList("dim1", "dim2"),
        Collections.singletonList("m2"),
        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
        9,
        1024
    );
  }
}
