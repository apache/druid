package org.apache.druid.indexing.common.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OverlordStatusMonitorTest {

  private TaskMaster taskMaster;
  private OverlordStatusMonitor monitor;

  @Before
  public void setUp() throws Exception {
    taskMaster = mock(TaskMaster.class);

    monitor = new OverlordStatusMonitor(taskMaster);
  }

  @Test
  public void testLeaderCount() {
    when(taskMaster.isLeader()).thenReturn(false);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals("leader/count", emitter.getEvents().get(0).toMap().get("metric"));
    Assert.assertEquals(0, emitter.getEvents().get(0).toMap().get("value"));

    when(taskMaster.isLeader()).thenReturn(true);
    emitter.flush();
    monitor.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals("leader/count", emitter.getEvents().get(0).toMap().get("metric"));
    Assert.assertEquals(1, emitter.getEvents().get(0).toMap().get("value"));
  }
}