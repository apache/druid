/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.MetadataSupervisorManager;
import io.druid.metadata.SQLMetadataSegmentManager;
import io.druid.metadata.TestDerbyConnector;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.TestHelper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import static org.easymock.EasyMock.expect;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static org.easymock.EasyMock.createMock;
import org.junit.rules.ExpectedException;

public class MaterializedViewSupervisorTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();
  private TestDerbyConnector derbyConnector;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private MetadataSupervisorManager metadataSupervisorManager;
  private SQLMetadataSegmentManager sqlMetadataSegmentManager;
  private TaskQueue taskQueue;
  private MaterializedViewSupervisor supervisor;
  private MaterializedViewSupervisorSpec spec;
  private ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  
  @Before
  public void setUp() throws IOException 
  {
    derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentTable();
    taskStorage = createMock(TaskStorage.class);
    taskMaster = createMock(TaskMaster.class);
    indexerMetadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector
    );
    metadataSupervisorManager = createMock(MetadataSupervisorManager.class);
    sqlMetadataSegmentManager = createMock(SQLMetadataSegmentManager.class);
    taskQueue = createMock(TaskQueue.class);
    taskQueue.start();
    objectMapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    spec = new MaterializedViewSupervisorSpec(
        "base",
        new DimensionsSpec(Lists.newArrayList(new StringDimensionSchema("dim")), null, null),
        new AggregatorFactory[]{new LongSumAggregatorFactory("m1", "m1")},
        HadoopTuningConfig.makeDefaultTuningConfig(),
        null,
        null,
        null,
        null,
        null,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlMetadataSegmentManager,
        indexerMetadataStorageCoordinator,
        new MaterializedViewTaskConfig()
    );
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
            ImmutableMap.<String, Object>of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-03",
            ImmutableMap.<String, Object>of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, null, null),
            9,
            1024
        )
    );
    expect(indexerMetadataStorageCoordinator.announceHistoricalSegments(baseSegments)).andVoid();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    Pair<SortedMap<Interval, String>, Map<Interval, List<DataSegment>>> toBuildInterval = supervisor.checkSegments();
    Map<Interval, List<DataSegment>> expectedSegments = Maps.newHashMap();
    expectedSegments.put(Intervals.of("2015-01-01T00Z/2015-01-02T00Z"), Lists.newArrayList(
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
            "2015-01-02",
            ImmutableMap.<String, Object>of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, null, null),
            9,
            1024
        ))
    );
    expectedSegments.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), Lists.newArrayList(
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-03",
            ImmutableMap.<String, Object>of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, null, null),
            9,
            1024
        ))
    );
    Assert.assertEquals(expectedSegments, toBuildInterval.rhs);
  }
}
