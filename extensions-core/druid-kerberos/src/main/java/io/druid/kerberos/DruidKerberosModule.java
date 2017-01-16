/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.kerberos;

import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.multibindings.Multibinder;
import com.metamx.http.client.HttpClient;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.Global;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.guice.http.HttpClientModule;
import io.druid.initialization.DruidModule;
import io.druid.server.initialization.jetty.ServletFilterHolder;

import java.util.List;
import java.util.Properties;

/**
 */
public class DruidKerberosModule implements DruidModule
{

  private static final String PROPERTY_AUTH_TYPE = "druid.authentication.type";
  private static final String KERBEROS = "kerberos";

  @Inject
  private Properties props;

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (isKerberosSecurityEnabled()) {
      JsonConfigProvider.bind(binder, "druid.hadoop.security.kerberos", DruidKerberosConfig.class);
      JsonConfigProvider.bind(binder, "druid.hadoop.security.spnego", SpnegoFilterConfig.class);

      Multibinder.newSetBinder(binder, ServletFilterHolder.class)
                 .addBinding()
                 .to(SpnegoFilterHolder.class);
      JsonConfigProvider.bind(binder, "druid.global.http", DruidHttpClientConfig.class, Global.class);
      binder.bind(HttpClient.class)
            .annotatedWith(Global.class)
            .toProvider(new KerberosHttpClientProvider(new HttpClientModule.HttpClientProvider(Global.class)))
            .in(LazySingleton.class);

      JsonConfigProvider.bind(binder, "druid.broker.http", DruidHttpClientConfig.class, Client.class);

      binder.bind(HttpClient.class)
            .annotatedWith(Client.class)
            .toProvider(new KerberosHttpClientProvider(new HttpClientModule.HttpClientProvider(Client.class)))
            .in(LazySingleton.class);
    }
  }

  private boolean isKerberosSecurityEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return props.getProperty(PROPERTY_AUTH_TYPE, "").equalsIgnoreCase(KERBEROS);
  }


}
