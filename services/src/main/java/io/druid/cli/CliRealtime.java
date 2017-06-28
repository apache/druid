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

package io.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.RealtimeModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.LookupModule;
import io.druid.server.initialization.jetty.ChatHandlerServerModule;

import java.util.List;
import java.util.Properties;

/**
 */
@Command(
    name = "realtime",
    description = "Runs a realtime node, see http://druid.io/docs/latest/Realtime.html for a description"
)
public class CliRealtime extends ServerRunnable
{
  private static final Logger log = new Logger(CliRealtime.class);

  @Inject
  private Properties properties;

  public CliRealtime()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new RealtimeModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/realtime");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8084);
          }
        },
        new ChatHandlerServerModule(properties),
        new LookupModule()
    );
  }
}
