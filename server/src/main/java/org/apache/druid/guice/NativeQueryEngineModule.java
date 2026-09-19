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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.util.Modules;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.NoopQuerySegmentWalker;
import org.apache.druid.server.system.module.SystemTableModule;
import org.apache.druid.server.system.module.SystemTableQueryResourceModule;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Facade for the modules that provide native query execution infrastructure.
 */
public class NativeQueryEngineModule implements DruidModule
{
  private final Module executionModule;
  private final Module queryResourceModule;

  private NativeQueryEngineModule(
      final Module executionModule,
      final Module queryResourceModule
  )
  {
    this.executionModule = executionModule;
    this.queryResourceModule = queryResourceModule;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static final class Builder
  {
    private Module overrideModule = binder -> {};
    @Nullable
    private Module queryResourceModule;
    private boolean scanOnly;

    private Builder()
    {
    }

    /**
     * Uses the minimum infrastructure required to serve Scan queries. This profile does not install processing or
     * merge-buffer dependencies.
     */
    public Builder scanOnly()
    {
      scanOnly = true;
      return this;
    }

    /** Applies role-specific overrides to the query execution bindings. */
    public Builder withOverrideModule(final Module module)
    {
      overrideModule = module;
      return this;
    }

    /** Replaces the standard native query HTTP resource with a role-specific resource module. */
    public Builder withQueryResourceModule(final Module module)
    {
      queryResourceModule = module;
      return this;
    }

    public NativeQueryEngineModule build()
    {
      final Module queryRunnerFactoryModule;
      final Module querySegmentWalkerModule;
      if (scanOnly) {
        queryRunnerFactoryModule = new QueryRunnerFactoryModule(ImmutableSet.of(ScanQuery.class));
        // Scan-only servers resolve node-local system tables without walking segments. Modules that need a real
        // walker, such as SegmentSchemaCacheModule, replace this optional default with an explicit binding.
        querySegmentWalkerModule = binder -> OptionalBinder.newOptionalBinder(binder, QuerySegmentWalker.class)
                                                           .setDefault()
                                                           .to(NoopQuerySegmentWalker.class)
                                                           .in(LazySingleton.class);
      } else {
        queryRunnerFactoryModule = new QueryRunnerFactoryModule();
        querySegmentWalkerModule = binder -> {};
      }

      return new NativeQueryEngineModule(
          Modules.override(
              Modules.combine(
                  new QueryableModule(),
                  queryRunnerFactoryModule,
                  querySegmentWalkerModule
              )
          ).with(overrideModule),
          queryResourceModule == null
          ? scanOnly ? new SystemTableQueryResourceModule() : new QueryResourceModule()
          : queryResourceModule
      );
    }
  }

  @Override
  public void configure(final Binder binder)
  {
    binder.install(executionModule);
    binder.install(new SegmentWranglerModule());
    binder.install(new JoinableFactoryModule());
    binder.install(new SystemTableModule());
    binder.install(queryResourceModule);
  }

  @Override
  public List<com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return new QueryableModule().getJacksonModules();
  }
}
