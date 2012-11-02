package com.metamx.druid.merger.coordinator.scaling;

import com.amazonaws.services.ec2.model.Instance;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.druid.merger.coordinator.WorkerWrapper;
import com.metamx.druid.merger.coordinator.config.EC2AutoScalingStrategyConfig;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * This class just logs when scaling should occur.
 */
public class NoopScalingStrategy implements ScalingStrategy<String>
{
  private static final EmittingLogger log = new EmittingLogger(NoopScalingStrategy.class);

  @Override
  public AutoScalingData<String> provision()
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
