package com.metamx.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.guice.annotations.Global;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.net.ssl.SSLContext;
import javax.validation.constraints.Min;
import java.lang.annotation.Annotation;

/**
 */
public class HttpClientModule implements Module
{
  public static HttpClientModule global()
  {
    return new HttpClientModule("druid.global.http", Global.class);
  }

  private final String propertyPrefix;
  private Annotation annotation = null;
  private Class<? extends Annotation> annotationClazz = null;

  public HttpClientModule(String propertyPrefix)
  {
    this.propertyPrefix = propertyPrefix;
  }

  public HttpClientModule(String propertyPrefix, Class<? extends Annotation> annotation)
  {
    this.propertyPrefix = propertyPrefix;
    this.annotationClazz = annotation;
  }

  public HttpClientModule(String propertyPrefix, Annotation annotation)
  {
    this.propertyPrefix = propertyPrefix;
    this.annotation = annotation;
  }

  @Override
  public void configure(Binder binder)
  {
    if (annotation != null) {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class, annotation);
      binder.bind(HttpClient.class)
            .annotatedWith(annotation)
            .toProvider(new HttpClientProvider(annotation))
            .in(LazySingleton.class);
    }
    else if (annotationClazz != null) {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class, annotationClazz);
      binder.bind(HttpClient.class)
            .annotatedWith(annotationClazz)
            .toProvider(new HttpClientProvider(annotationClazz))
            .in(LazySingleton.class);
    }
    else {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class);
      binder.bind(HttpClient.class)
            .toProvider(new HttpClientProvider())
            .in(LazySingleton.class);
    }
  }

  public static class DruidHttpClientConfig
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
      return readTimeout == null ? null : readTimeout.toStandardDuration();
    }
  }

  public static class HttpClientProvider implements Provider<HttpClient>
  {
    private final Key<Supplier<DruidHttpClientConfig>> configKey;
    private final Key<SSLContext> sslContextKey;

    private Provider<Supplier<DruidHttpClientConfig>> configProvider;
    private Provider<Lifecycle> lifecycleProvider;
    private Binding<SSLContext> sslContextBinding;

    public HttpClientProvider()
    {
      configKey = Key.get(new TypeLiteral<Supplier<DruidHttpClientConfig>>(){});
      sslContextKey = Key.get(SSLContext.class);
    }

    public HttpClientProvider(Annotation annotation)
    {
      configKey = Key.get(new TypeLiteral<Supplier<DruidHttpClientConfig>>(){}, annotation);
      sslContextKey = Key.get(SSLContext.class, annotation);
    }

    public HttpClientProvider(Class<? extends Annotation> annotation)
    {
      configKey = Key.get(new TypeLiteral<Supplier<DruidHttpClientConfig>>(){}, annotation);
      sslContextKey = Key.get(SSLContext.class, annotation);
    }

    @Inject
    public void configure(Injector injector)
    {
      configProvider = injector.getProvider(configKey);
      sslContextBinding = injector.getExistingBinding(sslContextKey);
      lifecycleProvider = injector.getProvider(Lifecycle.class);
    }

    @Override
    public HttpClient get()
    {
      final DruidHttpClientConfig config = configProvider.get().get();

      final HttpClientConfig.Builder builder = HttpClientConfig
          .builder()
          .withNumConnections(config.getNumConnections())
          .withReadTimeout(config.getReadTimeout());

      if (sslContextBinding != null) {
        builder.withSslContext(sslContextBinding.getProvider().get());
      }

      return HttpClientInit.createClient(builder.build(), lifecycleProvider.get());
    }
  }
}
