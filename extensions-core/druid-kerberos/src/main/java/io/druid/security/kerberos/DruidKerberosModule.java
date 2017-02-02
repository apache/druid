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

package io.druid.security.kerberos;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import com.metamx.http.client.HttpClient;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.Global;
import io.druid.guice.http.HttpClientModule;
import io.druid.guice.http.JettyHttpClientModule;
import io.druid.initialization.DruidModule;
import io.druid.server.initialization.jetty.ServletFilterHolder;
import io.druid.server.router.Router;

import java.util.List;

/**
 */
public class DruidKerberosModule implements DruidModule
{

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.hadoop.security.kerberos", AuthenticationKerberosConfig.class);
    JsonConfigProvider.bind(binder, "druid.hadoop.security.spnego", SpnegoFilterConfig.class);

    Multibinder.newSetBinder(binder, ServletFilterHolder.class)
               .addBinding()
               .to(SpnegoFilterHolder.class);

    binder.bind(HttpClient.class)
          .annotatedWith(Global.class)
          .toProvider(new KerberosHttpClientProvider(new HttpClientModule.HttpClientProvider(Global.class)))
          .in(LazySingleton.class);

    binder.bind(HttpClient.class)
          .annotatedWith(Client.class)
          .toProvider(new KerberosHttpClientProvider(new HttpClientModule.HttpClientProvider(Client.class)))
          .in(LazySingleton.class);

    binder.bind(org.eclipse.jetty.client.HttpClient.class)
          .annotatedWith(Router.class)
          .toProvider(new KerberosJettyHttpClientProvider(new JettyHttpClientModule.HttpClientProvider(Router.class)))
          .in(LazySingleton.class);
  }
}
