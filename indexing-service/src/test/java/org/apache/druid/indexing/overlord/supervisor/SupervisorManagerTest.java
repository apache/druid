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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.TestSeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(EasyMockRunner.class)
public class SupervisorManagerTest extends EasyMockSupport
{
  @Mock
  private MetadataSupervisorManager metadataSupervisorManager;

  @Mock
  private Supervisor supervisor1;

  @Mock
  private Supervisor supervisor2;

  @Mock
  private Supervisor supervisor3;

  private SupervisorManager manager;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp()
  {
    manager = new SupervisorManager(metadataSupervisorManager);
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
    supervisor1.start();
    replayAll();

    manager.start();
    Assert.assertEquals(1, manager.getSupervisorIds().size());

    manager.createOrUpdateAndStartSupervisor(spec);
    Assert.assertEquals(2, manager.getSupervisorIds().size());
    Assert.assertEquals(spec, manager.getSupervisorSpec("id1").get());
    verifyAll();

    resetAll();
    metadataSupervisorManager.insert("id1", spec2);
    supervisor2.start();
    supervisor1.stop(true);
    replayAll();

    manager.createOrUpdateAndStartSupervisor(spec2);
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
    supervisor3.stop(false);
    replayAll();

    manager.stop();
    verifyAll();

    Assert.assertTrue(manager.getSupervisorIds().isEmpty());
  }

  @Test
  public void testCreateOrUpdateAndStartSupervisorNotStarted()
  {
    exception.expect(IllegalStateException.class);
    manager.createOrUpdateAndStartSupervisor(new TestSupervisorSpec("id", null));
  }

  @Test
  public void testCreateOrUpdateAndStartSupervisorNullSpec()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    replayAll();

    exception.expect(NullPointerException.class);

    manager.start();
    manager.createOrUpdateAndStartSupervisor(null);
    verifyAll();
  }

  @Test
  public void testCreateOrUpdateAndStartSupervisorNullSpecId()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(ImmutableMap.of());
    replayAll();

    exception.expect(NullPointerException.class);

    manager.start();
    manager.createOrUpdateAndStartSupervisor(new TestSupervisorSpec(null, null));
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

    EasyMock.expect(metadataSupervisorManager.getAllForId(id)).andReturn(supervisorHistory);
    replayAll();

    List<VersionedSupervisorSpec> history = manager.getSupervisorHistoryForId(id);
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
    EasyMock.expect(supervisor1.getStatus()).andReturn(report);
    replayAll();

    manager.start();

    Assert.assertEquals(Optional.absent(), manager.getSupervisorStatus("non-existent-id"));
    Assert.assertEquals(report, manager.getSupervisorStatus("id1").get());

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
    supervisor1.stop(true);
    supervisor2.start();
    EasyMock.expectLastCall().andThrow(new RuntimeException("supervisor failed to start"));
    replayAll();

    final SupervisorSpec testSpecOld = new TestSupervisorSpec("id1", supervisor1);
    final SupervisorSpec testSpecNew = new TestSupervisorSpec("id1", supervisor2);

    manager.start();
    try {
      manager.createOrUpdateAndStartSupervisor(testSpecOld);
      manager.createOrUpdateAndStartSupervisor(testSpecNew);
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
    supervisor1.stop(false);
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
    supervisor1.reset(EasyMock.anyObject(DataSourceMetadata.class));
    replayAll();

    manager.start();
    Assert.assertTrue("resetValidSupervisor", manager.resetSupervisor("id1", null));
    Assert.assertFalse("resetInvalidSupervisor", manager.resetSupervisor("nobody_home", null));

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
    supervisor1.start();
    replayAll();

    manager.start();
    Assert.assertEquals(1, manager.getSupervisorIds().size());

    manager.createOrUpdateAndStartSupervisor(spec);
    Assert.assertEquals(2, manager.getSupervisorIds().size());
    Assert.assertEquals(spec, manager.getSupervisorSpec("id1").get());
    verifyAll();

    // mock suspend, which stops supervisor1 and sets suspended state in metadata, flipping to supervisor2
    // in TestSupervisorSpec implementation of createSuspendedSpec
    resetAll();
    metadataSupervisorManager.insert(EasyMock.eq("id1"), EasyMock.capture(capturedInsert));
    supervisor2.start();
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
    supervisor3.stop(false);
    replayAll();

    manager.stop();
    verifyAll();

    Assert.assertTrue(manager.getSupervisorIds().isEmpty());
  }

  @Test
  public void testGetActiveSupervisorIdForDatasourceWithAppendLock()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(Collections.emptyMap());

    NoopSupervisorSpec noopSupervisorSpec = new NoopSupervisorSpec("noop", ImmutableList.of("noopDS"));
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec suspendedSpec = EasyMock.mock(SeekableStreamSupervisorSpec.class);
    Supervisor suspendedSupervisor = EasyMock.mock(SeekableStreamSupervisor.class);
    EasyMock.expect(suspendedSpec.getId()).andReturn("suspendedSpec").anyTimes();
    EasyMock.expect(suspendedSpec.isSuspended()).andReturn(true).anyTimes();
    EasyMock.expect(suspendedSpec.getDataSources()).andReturn(ImmutableList.of("suspendedDS")).anyTimes();
    EasyMock.expect(suspendedSpec.createSupervisor()).andReturn(suspendedSupervisor).anyTimes();
    EasyMock.expect(suspendedSpec.createAutoscaler(suspendedSupervisor)).andReturn(null).anyTimes();
    EasyMock.expect(suspendedSpec.getContext()).andReturn(null).anyTimes();
    EasyMock.replay(suspendedSpec);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec activeSpec = EasyMock.mock(SeekableStreamSupervisorSpec.class);
    Supervisor activeSupervisor = EasyMock.mock(SeekableStreamSupervisor.class);
    EasyMock.expect(activeSpec.getId()).andReturn("activeSpec").anyTimes();
    EasyMock.expect(activeSpec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(activeSpec.getDataSources()).andReturn(ImmutableList.of("activeDS")).anyTimes();
    EasyMock.expect(activeSpec.createSupervisor()).andReturn(activeSupervisor).anyTimes();
    EasyMock.expect(activeSpec.createAutoscaler(activeSupervisor)).andReturn(null).anyTimes();
    EasyMock.expect(activeSpec.getContext()).andReturn(null).anyTimes();
    EasyMock.replay(activeSpec);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec activeSpecWithConcurrentLocks = EasyMock.mock(SeekableStreamSupervisorSpec.class);
    Supervisor activeSupervisorWithConcurrentLocks = EasyMock.mock(SeekableStreamSupervisor.class);
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
    EasyMock.replay(activeSpecWithConcurrentLocks);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec activeAppendSpec = EasyMock.mock(SeekableStreamSupervisorSpec.class);
    Supervisor activeAppendSupervisor = EasyMock.mock(SeekableStreamSupervisor.class);
    EasyMock.expect(activeAppendSpec.getId()).andReturn("activeAppendSpec").anyTimes();
    EasyMock.expect(activeAppendSpec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(activeAppendSpec.getDataSources()).andReturn(ImmutableList.of("activeAppendDS")).anyTimes();
    EasyMock.expect(activeAppendSpec.createSupervisor()).andReturn(activeAppendSupervisor).anyTimes();
    EasyMock.expect(activeAppendSpec.createAutoscaler(activeAppendSupervisor)).andReturn(null).anyTimes();
    EasyMock.expect(activeAppendSpec.getContext()).andReturn(ImmutableMap.of(
        Tasks.TASK_LOCK_TYPE,
        TaskLockType.APPEND.name()
    )).anyTimes();
    EasyMock.replay(activeAppendSpec);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    // A supervisor with useConcurrentLocks set to false explicitly must not use an append lock
    SeekableStreamSupervisorSpec specWithUseConcurrentLocksFalse = EasyMock.mock(SeekableStreamSupervisorSpec.class);
    Supervisor supervisorWithUseConcurrentLocksFalse = EasyMock.mock(SeekableStreamSupervisor.class);
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
    EasyMock.replay(specWithUseConcurrentLocksFalse);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    replayAll();
    manager.start();

    Assert.assertFalse(manager.getActiveSupervisorIdForDatasourceWithAppendLock("nonExistent").isPresent());

    manager.createOrUpdateAndStartSupervisor(noopSupervisorSpec);
    Assert.assertFalse(manager.getActiveSupervisorIdForDatasourceWithAppendLock("noopDS").isPresent());

    manager.createOrUpdateAndStartSupervisor(suspendedSpec);
    Assert.assertFalse(manager.getActiveSupervisorIdForDatasourceWithAppendLock("suspendedDS").isPresent());

    manager.createOrUpdateAndStartSupervisor(activeSpec);
    Assert.assertFalse(manager.getActiveSupervisorIdForDatasourceWithAppendLock("activeDS").isPresent());

    manager.createOrUpdateAndStartSupervisor(activeAppendSpec);
    Assert.assertTrue(manager.getActiveSupervisorIdForDatasourceWithAppendLock("activeAppendDS").isPresent());

    manager.createOrUpdateAndStartSupervisor(activeSpecWithConcurrentLocks);
    Assert.assertTrue(manager.getActiveSupervisorIdForDatasourceWithAppendLock("activeConcurrentLocksDS").isPresent());

    manager.createOrUpdateAndStartSupervisor(specWithUseConcurrentLocksFalse);
    Assert.assertFalse(
        manager.getActiveSupervisorIdForDatasourceWithAppendLock("dsWithUseConcurrentLocksFalse").isPresent()
    );

    verifyAll();
  }

  @Test
  public void testRegisterUpgradedPendingSegmentOnSupervisor()
  {
    EasyMock.expect(metadataSupervisorManager.getLatest()).andReturn(Collections.emptyMap());

    NoopSupervisorSpec noopSpec = new NoopSupervisorSpec("noop", ImmutableList.of("noopDS"));
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());

    SeekableStreamSupervisorSpec streamingSpec = EasyMock.mock(SeekableStreamSupervisorSpec.class);
    SeekableStreamSupervisor streamSupervisor = EasyMock.mock(SeekableStreamSupervisor.class);
    streamSupervisor.registerNewVersionOfPendingSegment(EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.expect(streamingSpec.getId()).andReturn("sss").anyTimes();
    EasyMock.expect(streamingSpec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.expect(streamingSpec.getDataSources()).andReturn(ImmutableList.of("DS")).anyTimes();
    EasyMock.expect(streamingSpec.createSupervisor()).andReturn(streamSupervisor).anyTimes();
    EasyMock.expect(streamingSpec.createAutoscaler(streamSupervisor)).andReturn(null).anyTimes();
    EasyMock.expect(streamingSpec.getContext()).andReturn(null).anyTimes();
    EasyMock.replay(streamingSpec);
    metadataSupervisorManager.insert(EasyMock.anyString(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    replayAll();

    final PendingSegmentRecord pendingSegment = new PendingSegmentRecord(
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

    manager.createOrUpdateAndStartSupervisor(noopSpec);
    Assert.assertFalse(manager.registerUpgradedPendingSegmentOnSupervisor("noop", pendingSegment));

    manager.createOrUpdateAndStartSupervisor(streamingSpec);
    Assert.assertTrue(manager.registerUpgradedPendingSegmentOnSupervisor("sss", pendingSegment));

    verifyAll();
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
      return null;
    }

    @Override
    public String getSource()
    {
      return null;
    }

    @Override
    public List<String> getDataSources()
    {
      return new ArrayList<>();
    }
  }
}
