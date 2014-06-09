package io.druid.server.initialization;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.NoopEmitter;
import io.druid.guice.ManageLifecycle;

/**
 */
public class NoopEmitterModule implements Module
{
  public static final String EMITTER_TYPE = "noop";

  @Override
  public void configure(Binder binder)
  {
  }

  @Provides
  @ManageLifecycle
  @Named(EMITTER_TYPE)
  public Emitter makeEmitter()
  {
    return new NoopEmitter();
  }
}
