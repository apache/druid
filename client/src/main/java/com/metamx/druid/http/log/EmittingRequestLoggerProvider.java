package com.metamx.druid.http.log;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamx.emitter.service.ServiceEmitter;

import javax.validation.constraints.NotNull;

/**
 */
@JsonTypeName("emitter")
public class EmittingRequestLoggerProvider implements RequestLoggerProvider
{
  @JsonProperty
  @NotNull
  private String feed = null;

  @JacksonInject
  @NotNull
  private ServiceEmitter emitter = null;

  @Inject
  public void injectMe(Injector injector)
  {
    System.out.println("YAYAYAYAYAYA!!!");
  }

  @Override
  public RequestLogger get()
  {
    return new EmittingRequestLogger(emitter, feed);
  }
}
