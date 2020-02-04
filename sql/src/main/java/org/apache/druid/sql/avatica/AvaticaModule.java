/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.avatica;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.server.initialization.jetty.JettyBindings;
import org.apache.druid.server.metrics.MetricsModule;

/**
 * The module responsible for providing bindings to Avatica.
 */
public class AvaticaModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.sql.avatica", AvaticaServerConfig.class);
    binder.bind(AvaticaMonitor.class).in(LazySingleton.class);
    JettyBindings.addHandler(binder, DruidAvaticaHandler.class);
    MetricsModule.register(binder, AvaticaMonitor.class);
  }
}
