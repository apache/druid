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

package org.apache.druid.server.system.module;

import com.google.inject.Binder;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.system.handler.SystemTableQueryResource;

/** Registers the restricted native query HTTP resource used by scan-only nodes. */
public class SystemTableQueryResourceModule implements DruidModule
{
  @Override
  public void configure(final Binder binder)
  {
    binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(true));
    binder.bind(SystemTableQueryResource.class).in(LazySingleton.class);
    binder.bind(QueryCountStatsProvider.class).to(SystemTableQueryResource.class).in(LazySingleton.class);
    Jerseys.addResource(binder, SystemTableQueryResource.class);
    LifecycleModule.register(binder, SystemTableQueryResource.class);
  }
}
