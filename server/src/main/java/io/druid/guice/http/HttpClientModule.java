/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.guice.http;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.Global;

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
    } else if (annotationClazz != null) {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class, annotationClazz);
      binder.bind(HttpClient.class)
            .annotatedWith(annotationClazz)
            .toProvider(new HttpClientProvider(annotationClazz))
            .in(LazySingleton.class);
    } else {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class);
      binder.bind(HttpClient.class)
            .toProvider(new HttpClientProvider())
            .in(LazySingleton.class);
    }
  }

  public static class HttpClientProvider extends AbstractHttpClientProvider<HttpClient>
  {
    public HttpClientProvider()
    {
    }

    public HttpClientProvider(Annotation annotation)
    {
      super(annotation);
    }

    public HttpClientProvider(Class<? extends Annotation> annotationClazz)
    {
      super(annotationClazz);
    }

    @Override
    public HttpClient get()
    {
      final DruidHttpClientConfig config = getConfigProvider().get().get();

      final HttpClientConfig.Builder builder = HttpClientConfig
          .builder()
          .withNumConnections(config.getNumConnections())
          .withReadTimeout(config.getReadTimeout());

      if (getSslContextBinding() != null) {
        builder.withSslContext(getSslContextBinding().getProvider().get());
      }

      return HttpClientInit.createClient(builder.build(), getLifecycleProvider().get());
    }
  }
}
