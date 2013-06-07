package com.metamx.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import sun.net.www.http.HttpClient;

import javax.validation.constraints.Min;

/**
 */
public class HttpModule implements Module
{

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.global.http", DruidHttpClientConfig.class);
  }

  public abstract static class DruidHttpClientConfig
  {
    @JsonProperty
    @Min(0)
    private int numConnections = 5;

    public int getNumConnections()
    {
      return numConnections;
    }
  }

  @Provides @LazySingleton @ManageLifecycle
  public HttpClient makeHttpClient(DruidHttpClientConfig config)
  {
    return null; // TODO
  }

}
