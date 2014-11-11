package io.druid.metadata;

import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import io.druid.guice.ManageLifecycle;

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
