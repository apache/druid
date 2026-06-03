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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.ObjectMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.TestSeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.supervisor.BoundedStreamConfig;
import org.apache.druid.indexing.seekablestream.supervisor.LagAggregator;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIngestionSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SupervisorIOConfigBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.metrics.SupervisorStatsProvider;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;

@RunWith(EasyMockRunner.class)
public class SupervisorManagerTest extends EasyMockSupport
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Mock
  private MetadataSupervisorManager metadataSupervisorManager;

  @Mock
  private StreamSupervisor supervisor1;

  @Mock
  private StreamSupervisor supervisor2;

  @Mock
  private Supervisor supervisor3;

  private SupervisorManager manager;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp()
  {
    manager = new SupervisorManager(MAPPER, metadataSupervisorManager);
  }

  @Test
  public void testCreateUpdateAndRemoveSupervisor()
  {
    SupervisorSpec spec = new TestSupervisorSpec("id1", supervisor1);
    SupervisorSpec spec2 = new TestSupervisorSpec("id1", supervisor2);
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id3", new TestSupervisorSpec("id3", supervisor3)
    );

    Assert.assertTrue(manager.getSupervisorIds().isEmpty());

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    metadataSupervisorManager.insert("id1", spec);
    supervisor3.start();
    EasyMock.expect(supervisor3.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();

    manager.start();
    Assert.assertEquals(1, manager.getSupervisorIds().size());

    manager.createOrUpdateAndStartSupervisor(spec, false);
    Assert.assertEquals(2, manager.getSupervisorIds().size());
    Assert.assertEquals(spec, manager.getSupervisorSpec("id1").get());
    verifyAll();

    resetAll();
    supervisor2.start();
    EasyMock.expect(supervisor2.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.stop(true);
    replayAll();

    manager.createOrUpdateAndStartSupervisor(spec2, false);
    Assert.assertEquals(2, manager.getSupervisorIds().size());
    Assert.assertEquals(spec2, manager.getSupervisorSpec("id1").get());
    verifyAll();

    resetAll();
    metadataSupervisorManager.insert(EasyMock.eq("id1"), EasyMock.anyObject(NoopSupervisorSpec.class));
    supervisor2.stop(true);
    replayAll();

    boolean retVal = manager.stopAndRemoveSupervisor("id1");
    Assert.assertTrue(retVal);
    Assert.assertEquals(1, manager.getSupervisorIds().size());
    Assert.assertEquals(Optional.absent(), manager.getSupervisorSpec("id1"));
    verifyAll();

    resetAll();
    SettableFuture<Void> stopFuture = SettableFuture.create();
    stopFuture.set(null);
    EasyMock.expect(supervisor3.stopAsync()).andReturn(stopFuture);
    replayAll();

    manager.stop();
    verifyAll();

    Assert.assertTrue(manager.getSupervisorIds().isEmpty());
  }

  @Test
  public void testCreateOrUpdateAndStartSupervisorIllegalEvolution()
  {
    SupervisorSpec spec = new TestSupervisorSpec("id1", supervisor1)
    {
      @Override
      public void validateSpecUpdateTo(SupervisorSpec proposedSpec) throws DruidException
      {
        throw InvalidInput.exception("Illegal spec update proposed");
      }
    };
    SupervisorSpec spec2 = new TestSupervisorSpec("id1", supervisor2);

    Assert.assertTrue(manager.getSupervisorIds().isEmpty());

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    metadataSupervisorManager.insert("id1", spec);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();

    manager.start();
    Assert.assertEquals(0, manager.getSupervisorIds().size());

    manager.createOrUpdateAndStartSupervisor(spec, false);
    Assert.assertEquals(1, manager.getSupervisorIds().size());
    Assert.assertEquals(spec, manager.getSupervisorSpec("id1").get());
    verifyAll();

    resetAll();
    exception.expect(DruidException.class);
    replayAll();

    manager.createOrUpdateAndStartSupervisor(spec2, false);
    verifyAll();
  }

  @Test
  public void testCreateOrUpdateAndStartSupervisorNotStarted()
  {
    exception.expect(IllegalStateException.class);
    manager.createOrUpdateAndStartSupervisor(new TestSupervisorSpec("id", null), false);
  }

  @Test
  public void testCreateOrUpdateAndStartSupervisorNullSpec()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    replayAll();

    exception.expect(NullPointerException.class);

    manager.start();
    manager.createOrUpdateAndStartSupervisor(null, false);
    verifyAll();
  }

  @Test
  public void testCreateOrUpdateAndStartSupervisorNullSpecId()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    replayAll();

    exception.expect(NullPointerException.class);

    manager.start();
    manager.createOrUpdateAndStartSupervisor(new TestSupervisorSpec(null, null), false);
    verifyAll();
  }

  @Test
  public void testUnchangedSpecDoesNotRestart()
  {
    SupervisorSpec spec = new TestSupervisorSpec("id1", supervisor1);
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of("id1", spec);
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();
    manager.start();
    final SupervisorSpecUpdateResult result = manager.createOrUpdateAndStartSupervisor(spec, true);
    Assert.assertFalse(result.isModified());
    Assert.assertFalse(result.isRestarted());
    verifyAll();
  }

  @Test
  public void testNewSupervisorRequiresRestart()
  {
    SupervisorSpec spec2 = new TestSupervisorSpec("id2", supervisor2);
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", new TestSupervisorSpec("id1", supervisor1)
    );
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();
    manager.start();
    verifyAll();

    resetAll();
    supervisor2.start();
    EasyMock.expect(supervisor2.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    metadataSupervisorManager.insert(EasyMock.eq("id2"), EasyMock.anyObject());
    replayAll();
    final SupervisorSpecUpdateResult result = manager.createOrUpdateAndStartSupervisor(spec2, true);
    Assert.assertTrue(result.isModified());
    Assert.assertTrue(result.isRestarted());
    verifyAll();
  }

  @Test
  public void testCreateOrUpdateSkipRestart_changedNoRestart_persistsWithoutRestart()
  {
    final Capture<TestSupervisorSpec> capturedInsert = Capture.newInstance();
    final Map<String, SupervisorSpec> existingSpecs =
        ImmutableMap.of("id1", new VersionedTestSupervisorSpec("id1", supervisor1, 1));
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    // Persist is expected; supervisor1.stop(...) is NOT expected, so verifyAll() asserts no restart.
    metadataSupervisorManager.insert(EasyMock.eq("id1"), EasyMock.capture(capturedInsert));
    replayAll();

    manager.start();
    // version differs (byte change) but requireRestart() is false.
    final SupervisorSpec updated = new VersionedTestSupervisorSpec("id1", supervisor1, 2);
    final SupervisorSpecUpdateResult updateResult = manager.createOrUpdateAndStartSupervisor(updated, true);
    Assert.assertTrue(updateResult.isModified());
    Assert.assertFalse(updateResult.isRestarted());
    Assert.assertSame(updated, capturedInsert.getValue());
    Assert.assertSame(updated, manager.getSupervisorSpec("id1").get());
    verifyAll();
  }

  @Test
  public void testCreateOrUpdateSkipRestart_identicalSpec_isNoop()
  {
    final Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of("id1", new TestSupervisorSpec("id1", supervisor1));
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    // No insert and no stop expected for an identical spec.
    replayAll();

    manager.start();
    final SupervisorSpecUpdateResult updateResult =
        manager.createOrUpdateAndStartSupervisor(new TestSupervisorSpec("id1", supervisor1), true);
    Assert.assertFalse(updateResult.isModified());
    Assert.assertFalse(updateResult.isRestarted());
    verifyAll();
  }

  @Test
  public void testCreateOrUpdateSkipRestart_changedRequiringRestart_restartsAndPersists()
  {
    // The restart decision belongs to the running spec, so requireRestart=true is set on the existing spec.
    final Map<String, SupervisorSpec> existingSpecs =
        ImmutableMap.of("id1", new VersionedTestSupervisorSpec("id1", supervisor1, 1, true));
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();
    manager.start();
    verifyAll();

    resetAll();
    // A changed spec that requires a restart must be fully applied (stop + recreate + persist), even on the
    // skip-restart path — it must never be silently dropped or persisted without restarting.
    supervisor1.stop(true);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    metadataSupervisorManager.insert(EasyMock.eq("id1"), EasyMock.anyObject());
    replayAll();

    final SupervisorSpec updated = new VersionedTestSupervisorSpec("id1", supervisor1, 2);
    final SupervisorSpecUpdateResult updateResult = manager.createOrUpdateAndStartSupervisor(updated, true);
    Assert.assertTrue(updateResult.isModified());
    Assert.assertTrue(updateResult.isRestarted());
    Assert.assertSame(updated, manager.getSupervisorSpec("id1").get());
    verifyAll();
  }

  @Test
  public void testCreateOrUpdateSkipRestart_mergesBeforeDiffAndRestartDecision()
  {
    final MergingVersionedTestSupervisorSpec existing =
        new MergingVersionedTestSupervisorSpec("id1", supervisor1, 1);
    final Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of("id1", existing);
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();

    manager.start();
    final MergingVersionedTestSupervisorSpec updated =
        new MergingVersionedTestSupervisorSpec("id1", supervisor1, null);
    final SupervisorSpecUpdateResult updateResult = manager.createOrUpdateAndStartSupervisor(updated, true);
    Assert.assertFalse(updateResult.isModified());
    Assert.assertFalse(updateResult.isRestarted());
    Assert.assertEquals(Integer.valueOf(1), updated.getVersion());
    Assert.assertSame(existing, manager.getSupervisorSpec("id1").get());
    verifyAll();
  }

  @Test
  public void testCreateOrUpdateWithoutSkipRestart_forcesRestartWhenSpecUnchanged()
  {
    final Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of("id1", new TestSupervisorSpec("id1", supervisor1));
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();
    manager.start();
    verifyAll();

    resetAll();
    supervisor1.stop(true);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();

    final SupervisorSpecUpdateResult updateResult =
        manager.createOrUpdateAndStartSupervisor(new TestSupervisorSpec("id1", supervisor1), false);
    Assert.assertFalse(updateResult.isModified());
    Assert.assertTrue(updateResult.isRestarted());
    verifyAll();
  }

  @Test
  public void testShouldUpdateSupervisorIllegalEvolution()
  {
    SupervisorSpec spec = new TestSupervisorSpec("id1", supervisor1)
    {
      @Override
      public void validateSpecUpdateTo(SupervisorSpec proposedSpec) throws DruidException
      {
        throw InvalidInput.exception("Illegal spec update proposed");
      }
    };
    SupervisorSpec spec2 = new TestSupervisorSpec("id1", supervisor2);
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", spec
    );
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    exception.expect(DruidException.class);
    replayAll();
    manager.start();
    manager.createOrUpdateAndStartSupervisor(spec2, true);
    verifyAll();
  }

  @Test
  public void testStopAndRemoveSupervisorNotStarted()
  {
    exception.expect(IllegalStateException.class);
    manager.stopAndRemoveSupervisor("id");
  }

  @Test
  public void testStopAndRemoveSupervisorNullSpecId()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    replayAll();

    exception.expect(NullPointerException.class);

    manager.start();
    manager.stopAndRemoveSupervisor(null);
    verifyAll();
  }

  @Test
  public void testGetSupervisorHistory()
  {
    Map<String, List<VersionedSupervisorSpec>> supervisorHistory = ImmutableMap.of();

    EasyMock.expect(metadataSupervisorManager.getAll()).andReturn(supervisorHistory);
    replayAll();

    Map<String, List<VersionedSupervisorSpec>> history = manager.getSupervisorHistory();
    verifyAll();

    Assert.assertEquals(supervisorHistory, history);
  }

  @Test
  public void testGetSupervisorHistoryForId()
  {
    String id = "test-supervisor-1";
    List<VersionedSupervisorSpec> supervisorHistory = ImmutableList.of();

    EasyMock.expect(metadataSupervisorManager.getAllForId(id, null)).andReturn(supervisorHistory);
    replayAll();

    List<VersionedSupervisorSpec> history = manager.getSupervisorHistoryForId(id, null);
    verifyAll();

    Assert.assertEquals(supervisorHistory, history);
  }

  @Test
  public void testGetSupervisorHistoryForIdWithLimit()
  {
    String id = "test-supervisor-1";
    Integer limit = 5;
    List<VersionedSupervisorSpec> supervisorHistory = ImmutableList.of();

    EasyMock.expect(metadataSupervisorManager.getAllForId(id, limit)).andReturn(supervisorHistory);
    replayAll();

    List<VersionedSupervisorSpec> history = manager.getSupervisorHistoryForId(id, limit);
    verifyAll();

    Assert.assertEquals(supervisorHistory, history);
  }

  @Test
  public void testGetSupervisorStatus()
  {
    SupervisorReport<Void> report = new SupervisorReport<>("id1", DateTimes.nowUtc(), null);

    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", new TestSupervisorSpec("id1", supervisor1)
    );

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(supervisor1.getStatus()).andReturn(report);
    replayAll();

    manager.start();

    Assert.assertEquals(Optional.absent(), manager.getSupervisorStatus("non-existent-id"));
    Assert.assertEquals(report, manager.getSupervisorStatus("id1").get());

    verifyAll();
  }

  @Test
  public void testGetSupervisorStatsWithNoSupervisors()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    replayAll();

    manager.start();

    Collection<SupervisorStatsProvider.SupervisorStats> stats = manager.getSupervisorStats();
    Assert.assertTrue(stats.isEmpty());

    verifyAll();
  }

  @Test
  public void testGetSupervisorStatsWithActiveSupervisors()
  {
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", new TestSupervisorSpec("id1", supervisor1)
    );

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(supervisor1.getState()).andReturn(SupervisorStateManager.BasicState.RUNNING);
    replayAll();

    manager.start();

    Collection<SupervisorStatsProvider.SupervisorStats> stats = manager.getSupervisorStats();
    Assert.assertEquals(1, stats.size());

    SupervisorStatsProvider.SupervisorStats stat = stats.iterator().next();
    Assert.assertEquals("id1", stat.getSupervisorId());
    Assert.assertEquals("TestSupervisorSpec", stat.getType());
    Assert.assertEquals("RUNNING", stat.getState());
    Assert.assertEquals("id1", stat.getDataSource());
    Assert.assertEquals("", stat.getStream());
    Assert.assertEquals("RUNNING", stat.getDetailedState());

    verifyAll();
  }

  @Test
  public void testGetSupervisorStatsWithNullState()
  {
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", new TestSupervisorSpec("id1", supervisor1)
    );

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(supervisor1.getState()).andReturn(null);
    replayAll();

    manager.start();

    Collection<SupervisorStatsProvider.SupervisorStats> stats = manager.getSupervisorStats();
    Assert.assertEquals(1, stats.size());

    SupervisorStatsProvider.SupervisorStats stat = stats.iterator().next();
    Assert.assertEquals("id1", stat.getSupervisorId());
    Assert.assertEquals("TestSupervisorSpec", stat.getType());
    Assert.assertEquals("UNKNOWN", stat.getState());
    Assert.assertEquals("UNKNOWN", stat.getDetailedState());

    verifyAll();
  }

  @Test
  public void testGetSupervisorStatsSkipsEntryWithNullSupervisor() throws Exception
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    replayAll();

    manager.start();

    final ConcurrentHashMap<String, Pair<Supervisor, SupervisorSpec>> supervisorsMap = getSupervisorsMap();
    supervisorsMap.put("id1", new Pair<>(null, new TestSupervisorSpec("id1", supervisor1)));

    final Collection<SupervisorStatsProvider.SupervisorStats> stats = manager.getSupervisorStats();
    Assert.assertTrue(stats.isEmpty());

    verifyAll();
  }

  @Test
  public void testGetSupervisorStatsSkipsNullEntryButRetainsValid() throws Exception
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    EasyMock.expect(supervisor2.getState()).andReturn(SupervisorStateManager.BasicState.RUNNING);
    replayAll();

    manager.start();

    final ConcurrentHashMap<String, Pair<Supervisor, SupervisorSpec>> supervisorsMap = getSupervisorsMap();
    supervisorsMap.put("id1", new Pair<>(null, new TestSupervisorSpec("id1", supervisor1)));
    supervisorsMap.put("id2", new Pair<>(supervisor2, new TestSupervisorSpec("id2", supervisor2)));

    final Collection<SupervisorStatsProvider.SupervisorStats> stats = manager.getSupervisorStats();
    Assert.assertEquals(1, stats.size());

    final SupervisorStatsProvider.SupervisorStats stat = stats.iterator().next();
    Assert.assertEquals("id2", stat.getSupervisorId());
    Assert.assertEquals("RUNNING", stat.getState());

    verifyAll();
  }

  @SuppressWarnings("unchecked")
  private ConcurrentHashMap<String, Pair<Supervisor, SupervisorSpec>> getSupervisorsMap() throws Exception
  {
    final Field field = SupervisorManager.class.getDeclaredField("supervisors");
    field.setAccessible(true);
    return (ConcurrentHashMap<String, Pair<Supervisor, SupervisorSpec>>) field.get(manager);
  }

  @Test
  public void testHandoffTaskGroupsEarly()
  {
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", new TestSupervisorSpec("id1", supervisor1)
    );

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.handoffTaskGroupsEarly(ImmutableList.of(1));

    replayAll();

    manager.start();

    Assert.assertTrue(manager.handoffTaskGroupsEarly("id1", ImmutableList.of(1)));
    Assert.assertFalse(manager.handoffTaskGroupsEarly("id2", ImmutableList.of(1)));

    verifyAll();
  }

  @Test
  public void testHandoffTaskGroupsEarlyOnNonStreamSupervisor()
  {
    final Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id3", new TestSupervisorSpec("id3", supervisor3)
    );

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor3.start();
    EasyMock.expect(supervisor3.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();

    replayAll();

    manager.start();

    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, () -> manager.handoffTaskGroupsEarly("id3", ImmutableList.of(1))),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.UNSUPPORTED,
            "general"
        ).expectMessageIs(
                "Operation[handoff] is not supported by supervisor[id3] of type[TestSupervisorSpec]."
        )
    );
    verifyAll();
  }

  @Test
  public void testStartAlreadyStarted()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    replayAll();

    exception.expect(IllegalStateException.class);

    manager.start();
    manager.start();
  }

  @Test
  public void testStartIndividualSupervisorsFailStart()
  {
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", new TestSupervisorSpec("id1", supervisor1),
        "id3", new TestSupervisorSpec("id3", supervisor3)
    );


    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor3.start();
    EasyMock.expect(supervisor3.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.start();
    EasyMock.expectLastCall().andThrow(new RuntimeException("supervisor explosion"));
    replayAll();

    manager.start();

    // if we get here, we are properly insulated from exploding supervisors
  }

  @Test
  public void testNoPersistOnFailedStart()
  {
    exception.expect(RuntimeException.class);

    Capture<TestSupervisorSpec> capturedInsert = Capture.newInstance();

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(Collections.emptyMap());
    metadataSupervisorManager.insert(EasyMock.eq("id1"), EasyMock.capture(capturedInsert));
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.stop(true);
    EasyMock.expect(supervisor2.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor2.start();
    EasyMock.expectLastCall().andThrow(new RuntimeException("supervisor failed to start"));
    replayAll();

    final SupervisorSpec testSpecOld = new TestSupervisorSpec("id1", supervisor1);
    final SupervisorSpec testSpecNew = new TestSupervisorSpec("id1", supervisor2);

    manager.start();
    try {
      manager.createOrUpdateAndStartSupervisor(testSpecOld, false);
      manager.createOrUpdateAndStartSupervisor(testSpecNew, false);
    }
    catch (Exception e) {
      Assert.assertEquals(testSpecOld, capturedInsert.getValue());
      throw e;
    }
  }

  @Test
  public void testStopThrowsException()
  {
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", new TestSupervisorSpec("id1", supervisor1)
    );

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.stopAsync();
    EasyMock.expectLastCall().andThrow(new RuntimeException("RTE"));
    replayAll();

    manager.start();
    manager.stop();
    verifyAll();
  }

  @Test
  public void testResetSupervisor()
  {
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", new TestSupervisorSpec("id1", supervisor1)
    );

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.reset(EasyMock.anyObject(DataSourceMetadata.class));
    replayAll();

    manager.start();
    Assert.assertTrue("resetValidSupervisor", manager.resetSupervisor("id1", null));
    Assert.assertFalse("resetInvalidSupervisor", manager.resetSupervisor("nobody_home", null));

    verifyAll();
  }

  @Test
  public void testResetOnNonStreamSupervisor()
  {
    final Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id3", new TestSupervisorSpec("id3", supervisor3)
    );

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor3.start();
    EasyMock.expect(supervisor3.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();

    manager.start();

    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, () -> manager.resetSupervisor("id3", null)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.UNSUPPORTED,
            "general"
        ).expectMessageIs(
            "Operation[reset] is not supported by supervisor[id3] of type[TestSupervisorSpec]."
        )
    );

    verifyAll();
  }

  @Test
  public void testResetSupervisorWithSpecificOffsets()
  {
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id1", new TestSupervisorSpec("id1", supervisor1)
    );

    DataSourceMetadata datasourceMetadata = new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of("0", "10", "1", "20", "2", "30"),
            ImmutableSet.of()
        )
    );

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.resetOffsets(datasourceMetadata);
    replayAll();

    manager.start();
    Assert.assertTrue("resetValidSupervisor", manager.resetSupervisor("id1", datasourceMetadata));
    Assert.assertFalse("resetInvalidSupervisor", manager.resetSupervisor("nobody_home", datasourceMetadata));

    verifyAll();
  }

  @Test
  public void testCreateSuspendResumeAndStopSupervisor()
  {
    Capture<TestSupervisorSpec> capturedInsert = Capture.newInstance();
    SupervisorSpec spec = new TestSupervisorSpec("id1", supervisor1, false, supervisor2);
    Map<String, SupervisorSpec> existingSpecs = ImmutableMap.of(
        "id3", new TestSupervisorSpec("id3", supervisor3)
    );

    // mock adding a supervisor to manager with existing supervisor then suspending it
    Assert.assertTrue(manager.getSupervisorIds().isEmpty());

    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(existingSpecs);
    metadataSupervisorManager.insert("id1", spec);
    supervisor3.start();
    EasyMock.expect(supervisor3.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();

    manager.start();
    Assert.assertEquals(1, manager.getSupervisorIds().size());

    manager.createOrUpdateAndStartSupervisor(spec, false);
    Assert.assertEquals(2, manager.getSupervisorIds().size());
    Assert.assertEquals(spec, manager.getSupervisorSpec("id1").get());
    verifyAll();

    // mock suspend, which stops supervisor1 and sets suspended state in metadata, flipping to supervisor2
    // in TestSupervisorSpec implementation of createSuspendedSpec
    resetAll();
    metadataSupervisorManager.insert(EasyMock.eq("id1"), EasyMock.capture(capturedInsert));
    supervisor2.start();
    EasyMock.expect(supervisor2.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    supervisor1.stop(true);
    replayAll();

    manager.suspendOrResumeSupervisor("id1", true);
    Assert.assertEquals(2, manager.getSupervisorIds().size());
    Assert.assertEquals(capturedInsert.getValue(), manager.getSupervisorSpec("id1").get());
    Assert.assertTrue(capturedInsert.getValue().suspended);
    verifyAll();

    // mock resume, which stops supervisor2 and sets suspended to false in metadata, flipping to supervisor1
    // in TestSupervisorSpec implementation of createRunningSpec
    resetAll();
    metadataSupervisorManager.insert(EasyMock.eq("id1"), EasyMock.capture(capturedInsert));
    supervisor2.stop(true);
    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();

    manager.suspendOrResumeSupervisor("id1", false);
    Assert.assertEquals(2, manager.getSupervisorIds().size());
    Assert.assertEquals(capturedInsert.getValue(), manager.getSupervisorSpec("id1").get());
    Assert.assertFalse(capturedInsert.getValue().suspended);
    verifyAll();

    // mock stop of suspended then resumed supervisor
    resetAll();
    metadataSupervisorManager.insert(EasyMock.eq("id1"), EasyMock.anyObject(NoopSupervisorSpec.class));
    supervisor1.stop(true);
    replayAll();

    boolean retVal = manager.stopAndRemoveSupervisor("id1");
    Assert.assertTrue(retVal);
    Assert.assertEquals(1, manager.getSupervisorIds().size());
    Assert.assertEquals(Optional.absent(), manager.getSupervisorSpec("id1"));
    verifyAll();

    // mock manager shutdown to ensure supervisor 3 stops
    resetAll();
    SettableFuture<Void> stopFuture = SettableFuture.create();
    stopFuture.set(null);
    EasyMock.expect(supervisor3.stopAsync()).andReturn(stopFuture);
    replayAll();

    manager.stop();
    verifyAll();

    Assert.assertTrue(manager.getSupervisorIds().isEmpty());
  }

  @Test
  public void testGetActiveSupervisorIdsForDatasourceWithAppendLock()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(Collections.emptyMap());

    NoopSupervisorSpec noopSupervisorSpec = new NoopSupervisorSpec("noop", ImmutableList.of("noopDS"));
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec suspendedSpec = EasyMock.createNiceMock(SeekableStreamSupervisorSpec.class);
    Supervisor suspendedSupervisor = EasyMock.createNiceMock(SeekableStreamSupervisor.class);
    EasyMock.expect(suspendedSpec.getId()).andReturn("suspendedSpec").anyTimes();
    EasyMock.expect(suspendedSpec.isSuspended()).andReturn(true).anyTimes();
    EasyMock.expect(suspendedSpec.getDataSources()).andReturn(ImmutableList.of("suspendedDS")).anyTimes();
    EasyMock.expect(suspendedSpec.createSupervisor()).andReturn(suspendedSupervisor).anyTimes();
    EasyMock.expect(suspendedSpec.createAutoscaler(suspendedSupervisor)).andReturn(null).anyTimes();
    EasyMock.expect(suspendedSpec.getContext()).andReturn(null).anyTimes();
    EasyMock.replay(suspendedSpec, suspendedSupervisor);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec activeSpec = EasyMock.createNiceMock(SeekableStreamSupervisorSpec.class);
    Supervisor activeSupervisor = EasyMock.createNiceMock(SeekableStreamSupervisor.class);
    EasyMock.expect(activeSpec.getId()).andReturn("activeSpec").anyTimes();
    EasyMock.expect(activeSpec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(activeSpec.getDataSources()).andReturn(ImmutableList.of("activeDS")).anyTimes();
    EasyMock.expect(activeSpec.createSupervisor()).andReturn(activeSupervisor).anyTimes();
    EasyMock.expect(activeSpec.createAutoscaler(activeSupervisor)).andReturn(null).anyTimes();
    EasyMock.expect(activeSpec.getContext()).andReturn(null).anyTimes();
    EasyMock.replay(activeSpec, activeSupervisor);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec activeSpecWithConcurrentLocks = EasyMock.createNiceMock(SeekableStreamSupervisorSpec.class);
    Supervisor activeSupervisorWithConcurrentLocks = EasyMock.createNiceMock(SeekableStreamSupervisor.class);
    EasyMock.expect(activeSpecWithConcurrentLocks.getId()).andReturn("activeSpecWithConcurrentLocks").anyTimes();
    EasyMock.expect(activeSpecWithConcurrentLocks.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(activeSpecWithConcurrentLocks.getDataSources())
            .andReturn(ImmutableList.of("activeConcurrentLocksDS")).anyTimes();
    EasyMock.expect(activeSpecWithConcurrentLocks.createSupervisor())
            .andReturn(activeSupervisorWithConcurrentLocks).anyTimes();
    EasyMock.expect(activeSpecWithConcurrentLocks.createAutoscaler(activeSupervisorWithConcurrentLocks))
            .andReturn(null).anyTimes();
    EasyMock.expect(activeSpecWithConcurrentLocks.getContext())
            .andReturn(ImmutableMap.of(Tasks.USE_CONCURRENT_LOCKS, true)).anyTimes();
    EasyMock.replay(activeSpecWithConcurrentLocks, activeSupervisorWithConcurrentLocks);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec activeAppendSpec = EasyMock.createNiceMock(SeekableStreamSupervisorSpec.class);
    Supervisor activeAppendSupervisor = EasyMock.createNiceMock(SeekableStreamSupervisor.class);
    EasyMock.expect(activeAppendSpec.getId()).andReturn("activeAppendSpec").anyTimes();
    EasyMock.expect(activeAppendSpec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(activeAppendSpec.getDataSources()).andReturn(ImmutableList.of("activeAppendDS")).anyTimes();
    EasyMock.expect(activeAppendSpec.createSupervisor()).andReturn(activeAppendSupervisor).anyTimes();
    EasyMock.expect(activeAppendSpec.createAutoscaler(activeAppendSupervisor)).andReturn(null).anyTimes();
    EasyMock.expect(activeAppendSpec.getContext()).andReturn(ImmutableMap.of(
        Tasks.TASK_LOCK_TYPE,
        TaskLockType.APPEND.name()
    )).anyTimes();
    EasyMock.replay(activeAppendSpec, activeAppendSupervisor);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    // A supervisor with useConcurrentLocks set to false explicitly must not use an append lock
    SeekableStreamSupervisorSpec specWithUseConcurrentLocksFalse = EasyMock.createNiceMock(SeekableStreamSupervisorSpec.class);
    Supervisor supervisorWithUseConcurrentLocksFalse = EasyMock.createNiceMock(SeekableStreamSupervisor.class);
    EasyMock.expect(specWithUseConcurrentLocksFalse.getId()).andReturn("useConcurrentLocksFalse").anyTimes();
    EasyMock.expect(specWithUseConcurrentLocksFalse.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(specWithUseConcurrentLocksFalse.getDataSources())
            .andReturn(ImmutableList.of("dsWithuseConcurrentLocksFalse")).anyTimes();
    EasyMock.expect(specWithUseConcurrentLocksFalse.createSupervisor()).andReturn(supervisorWithUseConcurrentLocksFalse).anyTimes();
    EasyMock.expect(specWithUseConcurrentLocksFalse.createAutoscaler(supervisorWithUseConcurrentLocksFalse))
            .andReturn(null).anyTimes();
    EasyMock.expect(specWithUseConcurrentLocksFalse.getContext()).andReturn(ImmutableMap.of(
        Tasks.USE_CONCURRENT_LOCKS,
        false,
        Tasks.TASK_LOCK_TYPE,
        TaskLockType.APPEND.name()
    )).anyTimes();
    EasyMock.replay(specWithUseConcurrentLocksFalse, supervisorWithUseConcurrentLocksFalse);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    replayAll();
    manager.start();

    Assert.assertTrue(manager.getActiveSupervisorIdsForDatasourceWithAppendLock("nonExistent").isEmpty());

    manager.createOrUpdateAndStartSupervisor(noopSupervisorSpec, false);
    Assert.assertTrue(manager.getActiveSupervisorIdsForDatasourceWithAppendLock("noopDS").isEmpty());

    manager.createOrUpdateAndStartSupervisor(suspendedSpec, false);
    Assert.assertTrue(manager.getActiveSupervisorIdsForDatasourceWithAppendLock("activeDS").isEmpty());

    manager.createOrUpdateAndStartSupervisor(activeSpec, false);
    Assert.assertTrue(manager.getActiveSupervisorIdsForDatasourceWithAppendLock("activeDS").isEmpty());

    manager.createOrUpdateAndStartSupervisor(activeAppendSpec, false);
    Assert.assertFalse(manager.getActiveSupervisorIdsForDatasourceWithAppendLock("activeAppendDS").isEmpty());

    manager.createOrUpdateAndStartSupervisor(activeSpecWithConcurrentLocks, false);
    Assert.assertFalse(manager.getActiveSupervisorIdsForDatasourceWithAppendLock("activeConcurrentLocksDS").isEmpty());

    manager.createOrUpdateAndStartSupervisor(specWithUseConcurrentLocksFalse, false);
    Assert.assertTrue(
        manager.getActiveSupervisorIdsForDatasourceWithAppendLock("dsWithUseConcurrentLocksFalse").isEmpty()
    );

    verifyAll();
  }

  @Test
  public void testRegisterUpgradedPendingSegmentOnSupervisor()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(Collections.emptyMap());

    NoopSupervisorSpec noopSpec = new NoopSupervisorSpec("noop", ImmutableList.of("noopDS"));
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec streamingSpec = EasyMock.createNiceMock(SeekableStreamSupervisorSpec.class);
    SeekableStreamSupervisor streamSupervisor = EasyMock.createNiceMock(SeekableStreamSupervisor.class);
    EasyMock.expect(streamSupervisor.registerNewVersionOfPendingSegment(EasyMock.anyObject())).andReturn(1).once();
    EasyMock.expect(streamingSpec.getId()).andReturn("sss").anyTimes();
    EasyMock.expect(streamingSpec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(streamingSpec.getDataSources()).andReturn(ImmutableList.of("DS")).anyTimes();
    EasyMock.expect(streamingSpec.createSupervisor()).andReturn(streamSupervisor).anyTimes();
    EasyMock.expect(streamingSpec.createAutoscaler(streamSupervisor)).andReturn(null).anyTimes();
    EasyMock.expect(streamingSpec.getContext()).andReturn(null).anyTimes();
    EasyMock.replay(streamingSpec, streamSupervisor);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    replayAll();

    final PendingSegmentRecord pendingSegment = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(
            "DS",
            Intervals.ETERNITY,
            "version",
            new NumberedShardSpec(0, 0)
        ),
        "sequenceName",
        "prevSegmentId",
        "upgradedFromSegmentId",
        "taskAllocatorId"
    );
    manager.start();

    manager.createOrUpdateAndStartSupervisor(noopSpec, false);
    Assert.assertFalse(manager.registerUpgradedPendingSegmentOnSupervisor("noop", pendingSegment).isPresent());

    manager.createOrUpdateAndStartSupervisor(streamingSpec, false);
    Assert.assertEquals(OptionalInt.of(1), manager.registerUpgradedPendingSegmentOnSupervisor("sss", pendingSegment));

    verifyAll();
  }

  @Test
  public void test_isAnotherTaskGroupPublishingToPartitions_throwsException_ifSupervisorIdIsNull()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> manager.isAnotherTaskGroupPublishingToPartitions(null, "task1", null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs("'supervisorId' cannot be null")
    );
  }

  @Test
  public void test_isAnotherTaskGroupPublishingToPartitions_throwsException_ifSupervisorNotFound()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> manager.isAnotherTaskGroupPublishingToPartitions("supervisor1", "task1", null)
        ),
        DruidExceptionMatcher.notFound().expectMessageIs("Could not find supervisor[supervisor1]")
    );
  }

  @Test
  public void test_isAnotherTaskGroupPublishingToPartitions_returnsFalse_forNonStreamingSupervisor()
  {
    final String supervisorId = "supervisor1";
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(
        Map.of(supervisorId, new TestSupervisorSpec(supervisorId, supervisor1))
    );

    supervisor1.start();
    EasyMock.expect(supervisor1.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();

    manager.start();

    Assert.assertFalse(
        manager.isAnotherTaskGroupPublishingToPartitions(supervisorId, "task1", null)
    );
  }

  @Test
  public void test_isAnotherTaskGroupPublishingToPartitions_throwsException_ifMetadataIsNull()
  {
    final String supervisorId = "supervisor1";
    final SeekableStreamSupervisor<Integer, String, ByteEntity> seekableStreamSupervisor =
        EasyMock.createMock(SeekableStreamSupervisor.class);
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(
        Map.of(supervisorId, new TestSupervisorSpec(supervisorId, seekableStreamSupervisor))
    );

    seekableStreamSupervisor.start();
    EasyMock.expect(seekableStreamSupervisor.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();
    EasyMock.replay(seekableStreamSupervisor);

    manager.start();

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> manager.isAnotherTaskGroupPublishingToPartitions(supervisorId, "task1", null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Start metadata[null] of type[null] is not valid streaming metadata"
        )
    );
  }

  @Test
  public void test_isAnotherTaskGroupPublishingToPartitions_throwsException_ifMetadataIsInvalid()
  {
    final String supervisorId = "supervisor1";
    final SeekableStreamSupervisor<Integer, String, ByteEntity> seekableStreamSupervisor =
        EasyMock.createMock(SeekableStreamSupervisor.class);
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(
        Map.of(supervisorId, new TestSupervisorSpec(supervisorId, seekableStreamSupervisor))
    );

    seekableStreamSupervisor.start();
    EasyMock.expect(seekableStreamSupervisor.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    replayAll();
    EasyMock.replay(seekableStreamSupervisor);

    manager.start();

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> manager.isAnotherTaskGroupPublishingToPartitions(supervisorId, "task1", new ObjectMetadata("abc"))
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Start metadata[ObjectMetadata{theObject=abc}] of"
            + " type[class org.apache.druid.indexing.overlord.ObjectMetadata]"
            + " is not valid streaming metadata"
        )
    );
  }

  @Test
  public void test_isAnotherTaskGroupPublishingToPartitions()
  {
    final String supervisorId = "supervisor1";
    final SeekableStreamSupervisor<Integer, String, ByteEntity> seekableStreamSupervisor =
        EasyMock.createMock(SeekableStreamSupervisor.class);
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(
        Map.of(supervisorId, new TestSupervisorSpec(supervisorId, seekableStreamSupervisor))
    );

    seekableStreamSupervisor.start();
    EasyMock.expect(seekableStreamSupervisor.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();

    // Expect a readyTaskId for which no other group is currently publishing
    final String readyTaskId = "task1";
    EasyMock.expect(
        seekableStreamSupervisor.isAnotherTaskGroupPublishingToPartitions(
            EasyMock.eq(readyTaskId),
            EasyMock.anyObject()
        )
    ).andReturn(false).atLeastOnce();

    // Expect a conflictingTaskId for which another group is currently publishing
    final String conflictingTaskId = "task2";
    EasyMock.expect(
        seekableStreamSupervisor.isAnotherTaskGroupPublishingToPartitions(
            EasyMock.eq(conflictingTaskId),
            EasyMock.anyObject()
        )
    ).andReturn(true).atLeastOnce();

    replayAll();
    EasyMock.replay(seekableStreamSupervisor);
    manager.start();

    final DataSourceMetadata startMetadata = TestSeekableStreamDataSourceMetadata.start(
        "topic",
        Map.of("0", "10")
    );
    Assert.assertTrue(
        manager.isAnotherTaskGroupPublishingToPartitions(supervisorId, conflictingTaskId, startMetadata)
    );
    Assert.assertFalse(
        manager.isAnotherTaskGroupPublishingToPartitions(supervisorId, readyTaskId, startMetadata)
    );
  }

  @Test
  public void testResetToLatestAndBackfill() throws Exception
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    replayAll();
    manager.start();

    final ConcurrentHashMap<String, Pair<Supervisor, SupervisorSpec>> supervisorsMap = getSupervisorsMap();
    final SeekableStreamSupervisorSpec streamSpec = EasyMock.createNiceMock(SeekableStreamSupervisorSpec.class);
    final SeekableStreamSupervisor streamSupervisor = EasyMock.createNiceMock(SeekableStreamSupervisor.class);
    final SeekableStreamSupervisorIOConfig ioConfig = EasyMock.createNiceMock(SeekableStreamSupervisorIOConfig.class);

    // non-SeekableStream supervisor → IAE
    // Use a concrete anonymous Supervisor (not a mock) to reliably fail instanceof SeekableStreamSupervisor
    final Supervisor nonStreamSupervisor = new Supervisor()
    {
      @Override
      public void start()
      {
      }

      @Override
      public void stop(boolean stopGracefully)
      {
      }

      @Override
      public SupervisorReport getStatus()
      {
        return null;
      }

      @Override
      public SupervisorStateManager.State getState()
      {
        return null;
      }

      @Override
      public void reset(DataSourceMetadata dataSourceMetadata)
      {
      }
    };
    supervisorsMap.put("id1", Pair.of(nonStreamSupervisor, streamSpec));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> manager.resetToLatestAndBackfill("id1", null)
    );

    // useEarliestSequenceNumber=true → IAE
    supervisorsMap.put("id1", Pair.of(streamSupervisor, streamSpec));
    EasyMock.expect(streamSupervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(ioConfig.isUseEarliestSequenceNumber()).andReturn(true).once();
    EasyMock.replay(streamSupervisor, streamSpec, ioConfig);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> manager.resetToLatestAndBackfill("id1", null)
    );
    EasyMock.reset(streamSupervisor, streamSpec, ioConfig);

    // useConcurrentLocks not set (null context) → IAE
    EasyMock.expect(streamSupervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(ioConfig.isUseEarliestSequenceNumber()).andReturn(false).once();
    EasyMock.expect(streamSpec.getContext()).andReturn(null).once();
    EasyMock.replay(streamSupervisor, streamSpec, ioConfig);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> manager.resetToLatestAndBackfill("id1", null)
    );
    EasyMock.reset(streamSupervisor, streamSpec, ioConfig);

    // useConcurrentLocks=false → IAE
    EasyMock.expect(streamSupervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(ioConfig.isUseEarliestSequenceNumber()).andReturn(false).once();
    EasyMock.expect(streamSpec.getContext()).andReturn(ImmutableMap.of("useConcurrentLocks", false)).once();
    EasyMock.replay(streamSupervisor, streamSpec, ioConfig);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> manager.resetToLatestAndBackfill("id1", null)
    );
    EasyMock.reset(streamSupervisor, streamSpec, ioConfig);

    // useConcurrentLocks="true" (string) → accepted, fails at next guard (not RUNNING)
    EasyMock.expect(streamSupervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(ioConfig.isUseEarliestSequenceNumber()).andReturn(false).once();
    EasyMock.expect(streamSpec.getContext()).andReturn(ImmutableMap.of("useConcurrentLocks", "true")).once();
    EasyMock.expect(streamSupervisor.getState()).andReturn(SupervisorStateManager.BasicState.SUSPENDED).once();
    EasyMock.replay(streamSupervisor, streamSpec, ioConfig);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> manager.resetToLatestAndBackfill("id1", null)
    );
    EasyMock.reset(streamSupervisor, streamSpec, ioConfig);

    // taskLockType=APPEND → accepted, fails at next guard (not RUNNING)
    EasyMock.expect(streamSupervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(ioConfig.isUseEarliestSequenceNumber()).andReturn(false).once();
    EasyMock.expect(streamSpec.getContext()).andReturn(ImmutableMap.of("taskLockType", "APPEND")).once();
    EasyMock.expect(streamSupervisor.getState()).andReturn(SupervisorStateManager.BasicState.SUSPENDED).once();
    EasyMock.replay(streamSupervisor, streamSpec, ioConfig);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> manager.resetToLatestAndBackfill("id1", null)
    );
    EasyMock.reset(streamSupervisor, streamSpec, ioConfig);

    // supervisor not RUNNING → IAE
    EasyMock.expect(streamSupervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(ioConfig.isUseEarliestSequenceNumber()).andReturn(false).once();
    EasyMock.expect(streamSpec.getContext()).andReturn(ImmutableMap.of("useConcurrentLocks", true)).once();
    EasyMock.expect(streamSupervisor.getState()).andReturn(SupervisorStateManager.BasicState.SUSPENDED).once();
    EasyMock.replay(streamSupervisor, streamSpec, ioConfig);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> manager.resetToLatestAndBackfill("id1", null)
    );
    EasyMock.reset(streamSupervisor, streamSpec, ioConfig);

    // empty latest offsets → ISE
    EasyMock.expect(streamSupervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(ioConfig.isUseEarliestSequenceNumber()).andReturn(false).once();
    EasyMock.expect(streamSpec.getContext()).andReturn(ImmutableMap.of("useConcurrentLocks", true)).once();
    EasyMock.expect(streamSupervisor.getState()).andReturn(SupervisorStateManager.BasicState.RUNNING).once();
    streamSupervisor.updatePartitionLagFromStream();
    EasyMock.expectLastCall().once();
    EasyMock.expect(streamSupervisor.getLatestSequencesFromStream()).andReturn(ImmutableMap.of()).once();
    EasyMock.replay(streamSupervisor, streamSpec, ioConfig);
    Assert.assertThrows(
        IllegalStateException.class,
        () -> manager.resetToLatestAndBackfill("id1", null)
    );
    EasyMock.reset(streamSupervisor, streamSpec, ioConfig);

    // empty start offsets from metadata → ISE
    EasyMock.expect(streamSupervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(ioConfig.isUseEarliestSequenceNumber()).andReturn(false).once();
    EasyMock.expect(streamSpec.getContext()).andReturn(ImmutableMap.of("useConcurrentLocks", true)).once();
    EasyMock.expect(streamSupervisor.getState()).andReturn(SupervisorStateManager.BasicState.RUNNING).once();
    streamSupervisor.updatePartitionLagFromStream();
    EasyMock.expectLastCall().once();
    EasyMock.expect(streamSupervisor.getLatestSequencesFromStream()).andReturn(ImmutableMap.of("0", 100L)).once();
    EasyMock.expect(streamSupervisor.getOffsetsFromMetadataStorage()).andReturn(ImmutableMap.of()).once();
    EasyMock.replay(streamSupervisor, streamSpec, ioConfig);
    Assert.assertThrows(
        IllegalStateException.class,
        () -> manager.resetToLatestAndBackfill("id1", null)
    );

    verifyAll();
  }

  @Test
  public void testCreateBackfillSpec()
  {
    final TestBackfillSupervisorSpec.IOConfig ioConfig = new TestBackfillSupervisorSpec.IOConfig("test-stream", null, null);
    final TestBackfillSupervisorSpec.IngestionSpec ingestionSpec = new TestBackfillSupervisorSpec.IngestionSpec(ioConfig);
    final SeekableStreamSupervisorSpec sourceSpec = new TestBackfillSupervisorSpec("original-id", ingestionSpec);

    final BoundedStreamConfig boundedStreamConfig = new BoundedStreamConfig(
        ImmutableMap.of("0", 100L),
        ImmutableMap.of("0", 200L)
    );

    // Without overriding taskCount
    final SupervisorSpec backfillSpec = sourceSpec.createBackfillSpec("backfill-id", boundedStreamConfig, null);
    Assert.assertEquals("backfill-id", backfillSpec.getId());
    final TestBackfillSupervisorSpec backfillCast = (TestBackfillSupervisorSpec) backfillSpec;
    final BoundedStreamConfig actualConfig = backfillCast.getIoConfig().getBoundedStreamConfig();
    Assert.assertNotNull(actualConfig);
    Assert.assertEquals(ImmutableMap.of("0", 100L), actualConfig.getStartSequenceNumbers());
    Assert.assertEquals(ImmutableMap.of("0", 200L), actualConfig.getEndSequenceNumbers());
    Assert.assertEquals(1, backfillCast.getIoConfig().getTaskCount());

    // With overriding taskCount
    final SupervisorSpec backfillSpecWithCount = sourceSpec.createBackfillSpec("backfill-id-2", boundedStreamConfig, 5);
    Assert.assertEquals("backfill-id-2", backfillSpecWithCount.getId());
    final TestBackfillSupervisorSpec backfillWithCount = (TestBackfillSupervisorSpec) backfillSpecWithCount;
    Assert.assertEquals(5, backfillWithCount.getIoConfig().getTaskCount());
  }

  private static class TestSupervisorSpec implements SupervisorSpec
  {
    private final String id;
    private final Supervisor supervisor;
    private final boolean suspended;
    private final Supervisor suspendedSupervisor;


    TestSupervisorSpec(String id, Supervisor supervisor)
    {
      this(id, supervisor, false, null);
    }

    TestSupervisorSpec(String id, Supervisor supervisor, boolean suspended, Supervisor suspendedSupervisor)
    {
      this.id = id;
      this.supervisor = supervisor;
      this.suspended = suspended;
      this.suspendedSupervisor = suspendedSupervisor;
    }

    @Override
    public SupervisorSpec createSuspendedSpec()
    {
      return new TestSupervisorSpec(id, suspendedSupervisor, true, supervisor);
    }

    @Override
    public SupervisorSpec createRunningSpec()
    {
      return new TestSupervisorSpec(id, suspendedSupervisor, false, supervisor);
    }

    @Override
    public String getId()
    {
      return id;
    }

    @Override
    public Supervisor createSupervisor()
    {
      return supervisor;
    }

    @Override
    public boolean isSuspended()
    {
      return suspended;
    }

    @Override
    public String getType()
    {
      return "TestSupervisorSpec";
    }

    @Override
    public String getSource()
    {
      return null;
    }

    @Override
    public List<String> getDataSources()
    {
      return Collections.singletonList(id);
    }
  }

  @JsonTypeName("testBackfill")
  private static class TestBackfillSupervisorSpec extends SeekableStreamSupervisorSpec
  {
    @JsonCreator
    TestBackfillSupervisorSpec(
        @JsonProperty("id") String id,
        @JsonProperty("spec") IngestionSpec ingestionSpec
    )
    {
      super(
          id,
          ingestionSpec,
          ImmutableMap.of("useConcurrentLocks", true),
          false,
          null, null, null, null,
          MAPPER,
          null, null, null, null
      );
    }

    @Override
    public Supervisor createSupervisor()
    {
      return null;
    }

    @Override
    public String getType()
    {
      return "testBackfill";
    }

    @Override
    public String getSource()
    {
      return "test-stream";
    }

    @Override
    protected SeekableStreamSupervisorSpec toggleSuspend(boolean suspend)
    {
      return this;
    }

    @Override
    public SeekableStreamSupervisorSpec createBackfillSpec(
        String backfillId,
        BoundedStreamConfig boundedStreamConfig,
        @Nullable Integer taskCount
    )
    {
      return new TestBackfillSupervisorSpec(
          backfillId,
          new IngestionSpec(new IOConfig(getIoConfig().getStream(), taskCount, boundedStreamConfig))
      );
    }

    @Override
    public SeekableStreamSupervisorIOConfig getIoConfig()
    {
      return getSpec().getIOConfig();
    }

    @Override
    public Builder<?> toBuilder()
    {
      throw new UnsupportedOperationException();
    }

    @JsonTypeName("testBackfillIngestionSpec")
    static class IngestionSpec extends SeekableStreamSupervisorIngestionSpec
    {
      @JsonCreator
      IngestionSpec(
          @JsonProperty("ioConfig") IOConfig ioConfig
      )
      {
        super(
            DataSchema.builder()
                      .withDataSource("testDS")
                      .withTimestamp(new TimestampSpec("time", "auto", null))
                      .withDimensions(new DimensionsSpec(Collections.emptyList()))
                      .build(),
            ioConfig,
            null
        );
      }
    }

    @JsonTypeName("testBackfillIOConfig")
    static class IOConfig extends SeekableStreamSupervisorIOConfig
    {
      @JsonCreator
      IOConfig(
          @JsonProperty("stream") String stream,
          @JsonProperty("taskCount") Integer taskCount,
          @JsonProperty("boundedStreamConfig") BoundedStreamConfig boundedStreamConfig
      )
      {
        super(stream, null, 1, taskCount, null, null, null, false, null, null, null, null, LagAggregator.DEFAULT, null, null, null, null, boundedStreamConfig);
      }

      @Override
      public SupervisorIOConfigBuilder<?, ?> toBuilder()
      {
        return new SupervisorIOConfigBuilder.DefaultSupervisorIOConfigBuilder().copyFromBase(this);
      }
    }
  }

  /**
   * A {@link TestSupervisorSpec} with an explicitly-serialized {@code version} field, so two specs
   * sharing the same id can still differ in their serialized form (the base spec serializes only
   * id-derived properties).
   */
  private static class VersionedTestSupervisorSpec extends TestSupervisorSpec
  {
    private final int version;
    private final boolean requireRestart;

    VersionedTestSupervisorSpec(String id, Supervisor supervisor, int version)
    {
      this(id, supervisor, version, false);
    }

    VersionedTestSupervisorSpec(String id, Supervisor supervisor, int version, boolean requireRestart)
    {
      super(id, supervisor);
      this.version = version;
      this.requireRestart = requireRestart;
    }

    @JsonProperty
    public int getVersion()
    {
      return version;
    }

    /**
     * Lets a test model either a no-restart change (default) or a restart-requiring change. Invoked on the
     * running spec, so this models whether the running supervisor requires a restart. (The default
     * {@link SupervisorSpec#requireRestart} is conservative and returns true.)
     */
    @Override
    public boolean requireRestart(SupervisorSpec proposedSpec)
    {
      return requireRestart;
    }
  }

  private static class MergingVersionedTestSupervisorSpec extends TestSupervisorSpec
  {
    @Nullable
    private Integer version;

    MergingVersionedTestSupervisorSpec(
        final String id,
        final Supervisor supervisor,
        @Nullable final Integer version
    )
    {
      super(id, supervisor);
      this.version = version;
    }

    @JsonProperty
    @Nullable
    public Integer getVersion()
    {
      return version;
    }

    @Override
    public void merge(@Nullable final SupervisorSpec existingSpec)
    {
      if (version == null && existingSpec instanceof MergingVersionedTestSupervisorSpec) {
        version = ((MergingVersionedTestSupervisorSpec) existingSpec).version;
      }
    }
  }
}
