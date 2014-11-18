package io.druid.indexing.overlord.setup;

import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.overlord.ImmutableZkWorker;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.worker.Worker;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;

public class FillCapacityWithAffinityWorkerSelectStrategyTest
{
  @Test
  public void testFindWorkerForTask() throws Exception
  {
    FillCapacityWorkerSelectStrategy strategy = new FillCapacityWithAffinityWorkerSelectStrategy(
        new FillCapacityWithAffinityConfig(ImmutableMap.of("foo", Arrays.asList("localhost")))
    );

    Optional<ImmutableZkWorker> optional = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "lhost",
            new ImmutableZkWorker(
                new Worker("lhost", "lhost", 1, "v1"), 0,
                Sets.<String>newHashSet()
            ),
            "localhost",
            new ImmutableZkWorker(
                new Worker("localhost", "localhost", 1, "v1"), 0,
                Sets.<String>newHashSet()
            )
        ),
        new NoopTask(null, 1, 0, null, null)
        {
          @Override
          public String getDataSource()
          {
            return "foo";
          }
        }
    );
    ImmutableZkWorker worker = optional.get();
    Assert.assertEquals("localhost", worker.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWithNulls() throws Exception
  {
    FillCapacityWorkerSelectStrategy strategy = new FillCapacityWithAffinityWorkerSelectStrategy(
        new FillCapacityWithAffinityConfig(ImmutableMap.of("foo", Arrays.asList("localhost")))
    );

    Optional<ImmutableZkWorker> optional = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "lhost",
            new ImmutableZkWorker(
                new Worker("lhost", "lhost", 1, "v1"), 0,
                Sets.<String>newHashSet()
            ),
            "localhost",
            new ImmutableZkWorker(
                new Worker("localhost", "localhost", 1, "v1"), 0,
                Sets.<String>newHashSet()
            )
        ),
        new NoopTask(null, 1, 0, null, null)
    );
    ImmutableZkWorker worker = optional.get();
    Assert.assertEquals("lhost", worker.getWorker().getHost());
  }

  @Test
  public void testIsolation() throws Exception
  {
    FillCapacityWorkerSelectStrategy strategy = new FillCapacityWithAffinityWorkerSelectStrategy(
        new FillCapacityWithAffinityConfig(ImmutableMap.of("foo", Arrays.asList("localhost")))
    );

    Optional<ImmutableZkWorker> optional = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "localhost",
            new ImmutableZkWorker(
                new Worker("localhost", "localhost", 1, "v1"), 0,
                Sets.<String>newHashSet()
            )
        ),
        new NoopTask(null, 1, 0, null, null)
    );
    Assert.assertFalse(optional.isPresent());
  }
}