package org.apache.druid.server.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class TaskCountStatsMonitorTest
{
  private TaskCountStatsProvider statsProvider;

  @Before
  public void setUp()
  {
    statsProvider = new TaskCountStatsProvider()
    {
      @Override
      public Map<String, Long> getSuccessfulTaskCount()
      {
        return null;
      }

      @Override
      public Map<String, Long> getFailedTaskCount()
      {
        return null;
      }

      @Override
      public Map<String, Long> getRunningTaskCount()
      {
        return ImmutableMap.of("d1", 1L);
      }

      @Override
      public Map<String, Long> getPendingTaskCount()
      {
        return null;
      }

      @Override
      public Map<String, Long> getWaitingTaskCount()
      {
        return null;
      }
    };
  }

  @Test
  public void testMonitor()
  {
    final TaskCountStatsMonitor monitor = new TaskCountStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals("task/running/count", emitter.getEvents().get(0).toMap().get("metric"));
    Assert.assertEquals(1L, emitter.getEvents().get(0).toMap().get("value"));
  }
}
