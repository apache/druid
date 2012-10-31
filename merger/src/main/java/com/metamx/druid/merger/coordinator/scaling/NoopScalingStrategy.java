package com.metamx.druid.merger.coordinator.scaling;

import com.amazonaws.services.ec2.model.Instance;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.druid.merger.coordinator.WorkerWrapper;
import com.metamx.druid.merger.coordinator.config.S3AutoScalingStrategyConfig;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;

/**
 * This class just logs when scaling should occur.
 */
public class NoopScalingStrategy implements ScalingStrategy
{
  private static final EmittingLogger log = new EmittingLogger(NoopScalingStrategy.class);

  private final S3AutoScalingStrategyConfig config;

  private final Object lock = new Object();

  private volatile String currentlyProvisioning = null;
  private volatile String currentlyTerminating = null;

  public NoopScalingStrategy(
      S3AutoScalingStrategyConfig config
  )
  {
    this.config = config;
  }

  @Override
  public void provision(Map<String, WorkerWrapper> zkWorkers)
  {
    synchronized (lock) {
      if (currentlyProvisioning != null) {
        if (!zkWorkers.containsKey(currentlyProvisioning)) {
          log.info(
              "[%s] is still provisioning. Wait for it to finish before requesting new worker.",
              currentlyProvisioning
          );
          return;
        }
      }

      try {
        log.info("If I were a real strategy I'd create something now");
        currentlyProvisioning = "willNeverBeTrue";
      }
      catch (Exception e) {
        log.error(e, "Unable to create instance");
        currentlyProvisioning = null;
      }
    }
  }

  @Override
  public Instance terminateIfNeeded(Map<String, WorkerWrapper> zkWorkers)
  {
    synchronized (lock) {
      if (currentlyTerminating != null) {
        if (zkWorkers.containsKey(currentlyTerminating)) {
          log.info("[%s] has not terminated. Wait for it to finish before terminating again.", currentlyTerminating);
          return null;
        }
      }

      MinMaxPriorityQueue<WorkerWrapper> currWorkers = MinMaxPriorityQueue.orderedBy(
          new Comparator<WorkerWrapper>()
          {
            @Override
            public int compare(WorkerWrapper w1, WorkerWrapper w2)
            {
              DateTime w1Time = (w1 == null) ? new DateTime(0) : w1.getLastCompletedTaskTime();
              DateTime w2Time = (w2 == null) ? new DateTime(0) : w2.getLastCompletedTaskTime();
              return w1Time.compareTo(w2Time);
            }
          }
      ).create(
          zkWorkers.values()
      );

      if (currWorkers.size() <= config.getMinNuMWorkers()) {
        return null;
      }

      WorkerWrapper thatLazyWorker = currWorkers.poll();

      if (System.currentTimeMillis() - thatLazyWorker.getLastCompletedTaskTime().getMillis()
          > config.getMillisToWaitBeforeTerminating()) {
        try {
          log.info("If I were a real strategy I'd terminate something now");
          currentlyTerminating = "willNeverBeTrue";

          return null;
        }
        catch (Exception e) {
          log.error(e, "Unable to terminate instance");
          currentlyTerminating = null;
        }
      }

      return null;
    }
  }
}
