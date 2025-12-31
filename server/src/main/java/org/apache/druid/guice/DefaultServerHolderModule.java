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


package org.apache.druid.guice;

import com.google.inject.Binder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.ExcludeScope;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.metrics.NoopTaskHolder;
import org.apache.druid.java.util.metrics.TaskHolder;
import org.apache.druid.server.metrics.DefaultLoadSpecHolder;
import org.apache.druid.server.metrics.LoadSpecHolder;

/**
 * Binds the following holder configs for all servers except {@code CliPeon}:
 * <ul>
 * <li>{@link TaskHolder} to {@link NoopTaskHolder}</li>
 * <li>{@link LoadSpecHolder} to {@link DefaultLoadSpecHolder}</li>
 * </ul>
 *
 * <p>For {@code CliPeon}, these bindings are overridden by the peon-specific module.</p>
 */
@ExcludeScope(roles = {NodeRole.PEON_JSON_NAME})
public class DefaultServerHolderModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(TaskHolder.class).to(NoopTaskHolder.class).in(LazySingleton.class);
    binder.bind(LoadSpecHolder.class).to(DefaultLoadSpecHolder.class).in(LazySingleton.class);
  }
}
