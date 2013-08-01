package com.metamx.druid.http.log;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.druid.guice.annotations.Json;

import javax.validation.constraints.NotNull;
import java.io.File;

/**
 */
@JsonTypeName("file")
public class FileRequestLoggerProvider implements RequestLoggerProvider
{
  @JsonProperty
  @NotNull
  private File dir = null;

  @JacksonInject
  @NotNull
  private ScheduledExecutorFactory factory = null;


  @JacksonInject
  @NotNull
  @Json
  private ObjectMapper jsonMapper = null;

  @Override
  public RequestLogger get()
  {
    return new FileRequestLogger(jsonMapper, factory.create(1, "RequestLogger-%s"), dir);
  }
}
