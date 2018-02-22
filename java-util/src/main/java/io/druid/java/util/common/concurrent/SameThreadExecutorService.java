package io.druid.java.util.common.concurrent;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Phaser;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SameThreadExecutorService extends AbstractExecutorService
{
  private final AtomicBoolean shutdownLeader = new AtomicBoolean(true);
  private final Phaser shutdownPhaser = new Phaser(0);
  private final int initialPhase = shutdownPhaser.register();

  @Override
  public void shutdown()
  {
    if (shutdownLeader.getAndSet(false)) {
      shutdownPhaser.arriveAndDeregister();
    }
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    shutdown();
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown()
  {
    return !shutdownLeader.get();
  }

  @Override
  public boolean isTerminated()
  {
    return isShutdown() && shutdownPhaser.getRegisteredParties() < 1;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    try {
      shutdownPhaser.awaitAdvanceInterruptibly(initialPhase, timeout, unit);
      return true;
    }
    catch (TimeoutException ignored) {
      return false;
    }
  }

  @Override
  public void execute(Runnable command)
  {
    shutdownPhaser.register();
    try {
      if (isShutdown()) {
        throw new RejectedExecutionException();
      }
      command.run();
    }
    finally {
      shutdownPhaser.arriveAndDeregister();
    }
  }
}
