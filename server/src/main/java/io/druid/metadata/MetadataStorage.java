package io.druid.metadata;

import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;

public abstract class MetadataStorage
{
  @LifecycleStart
  public void start()
  {
    // do nothing
  }

  @LifecycleStop
  public void stop()
  {
    // do nothing
  }
}
