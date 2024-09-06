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

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.util.Providers;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;

import java.util.List;

/**
 * Contains bindings necessary for Coordinator to perform supervisor cleanup
 * when Coordinator and Overlord are running as separate processes.
 */
public class SupervisorCleanupModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    // These are needed to deserialize SupervisorSpec for Supervisor Auto Cleanup
    binder.bind(TaskStorage.class).toProvider(Providers.of(null));
    binder.bind(TaskMaster.class).toProvider(Providers.of(null));
    binder.bind(RowIngestionMetersFactory.class).toProvider(Providers.of(null));
    binder.bind(CompactionScheduler.class).toProvider(Providers.of(null));
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return new SupervisorModule().getJacksonModules();
  }
}
