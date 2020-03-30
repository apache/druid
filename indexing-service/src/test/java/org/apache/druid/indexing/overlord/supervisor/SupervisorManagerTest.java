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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.MetadataSupervisorManager;
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
