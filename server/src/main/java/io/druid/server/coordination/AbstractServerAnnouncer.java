package io.druid.server.coordination;

import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;

/**
 */
public abstract class AbstractServerAnnouncer implements ServerAnnouncer
{
  private volatile boolean started = false;

  private final Object lock = new Object();

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      announceSelf();
      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      unannounceSelf();
      started = false;
    }
  }
}
