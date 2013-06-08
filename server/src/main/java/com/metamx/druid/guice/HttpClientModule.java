package com.metamx.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.guice.annotations.Global;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.validation.constraints.Min;

/**
 */
public class HttpClientModule implements Module
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

    @JsonProperty
    private Period readTimeout = null;

    public int getNumConnections()
    {
      return numConnections;
    }

    public Duration getReadTimeout()
    {
      return readTimeout.toStandardDuration();
    }
  }

  @Provides @LazySingleton @Global
  public HttpClient makeHttpClient(DruidHttpClientConfig config, Lifecycle lifecycle, @Nullable SSLContext sslContext)
  {
    final HttpClientConfig.Builder builder = HttpClientConfig
        .builder()
        .withNumConnections(config.getNumConnections())
        .withReadTimeout(config.getReadTimeout());

    if (sslContext != null) {
      builder.withSslContext(sslContext);
    }

    return HttpClientInit.createClient(builder.build(), lifecycle);
  }


}
