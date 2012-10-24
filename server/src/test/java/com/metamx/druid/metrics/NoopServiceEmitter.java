package com.metamx.druid.metrics;

import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;

public class NoopServiceEmitter extends ServiceEmitter
{
  public NoopServiceEmitter()
  {
    super(null, null, null);
  }

  @Override
  public void emit(Event event) {}
}
