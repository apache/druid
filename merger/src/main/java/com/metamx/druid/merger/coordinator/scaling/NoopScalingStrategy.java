package com.metamx.druid.merger.coordinator.scaling;

import com.metamx.emitter.EmittingLogger;

import java.util.List;

/**
 * This class just logs when scaling should occur.
 */
public class NoopScalingStrategy implements ScalingStrategy<String>
{
  private static final EmittingLogger log = new EmittingLogger(NoopScalingStrategy.class);

  @Override
  public AutoScalingData<String> provision(long numUnassignedTasks)
  {
    log.info("If I were a real strategy I'd create something now");
    return null;
  }

  @Override
  public AutoScalingData<String> terminate(List<String> nodeIds)
  {
    log.info("If I were a real strategy I'd terminate %s now", nodeIds);
    return null;
  }
}
