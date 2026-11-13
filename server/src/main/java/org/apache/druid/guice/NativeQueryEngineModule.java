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
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.NoopQuerySegmentWalker;
import org.apache.druid.server.system.module.SystemTableModule;
import org.apache.druid.server.system.module.SystemTableQueryResourceModule;
import org.apache.druid.server.system.query.SystemTableQuerySegmentWalker;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;

/**
 * Facade for the modules that provide native query execution infrastructure.
 */
public class NativeQueryEngineModule implements DruidModule
{
  private final Module executionModule;
  private final Module queryResourceModule;
  private final boolean systemTableQueryEnabled;

  private NativeQueryEngineModule(
      final Module executionModule,
      final Module queryResourceModule,
      final boolean systemTableQueryEnabled
  )
  {
    this.executionModule = executionModule;
    this.queryResourceModule = queryResourceModule;
    this.systemTableQueryEnabled = systemTableQueryEnabled;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static final class Builder
  {
    private Module overrideModule = _ -> {};
    @Nullable
    private Module queryResourceModule;
    private boolean scanOnly;
    private boolean systemTableQueryEnabled;
    @Nullable
    private Class<? extends QuerySegmentWalker> querySegmentWalkerClass;

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

    /**
     * Binds the system tables, and on scan-only servers the native query endpoint, if
     * {@link SystemTableModule#ENABLE_DRUID_ENGINE_FOR_SYSTEM_TABLES_PROPERTY} is set in the given properties.
     */
    public Builder enableSystemTableQuery(final Properties properties)
    {
      return enableSystemTableQuery(SystemTableModule.isEnabled(properties));
    }

    /**
     * As {@link #enableSystemTableQuery(Properties)}, for callers that have already read the property.
     */
    public Builder enableSystemTableQuery(final boolean enable)
    {
      systemTableQueryEnabled = enable;
      return this;
    }

    /**
     * Binds {@link QuerySegmentWalker} to the given walker. Scan-only servers default to
     * {@link NoopQuerySegmentWalker}.
     * Servers that bind their walker themselves, such as the Broker, don't call this.
     *
     * <p>When system tables are enabled, the walker is wrapped in a {@link SystemTableQuerySegmentWalker}, which runs
     * queries on system tables and passes every other query to the given walker.</p>
     */
    public Builder withQuerySegmentWalker(final Class<? extends QuerySegmentWalker> walkerClass)
    {
      querySegmentWalkerClass = walkerClass;
      return this;
    }

    /**
     * Applies role-specific overrides to the query execution bindings.
     */
    public Builder withOverrideModule(final Module module)
    {
      overrideModule = module;
      return this;
    }

    /**
     * Replaces the standard native query HTTP resource with a role-specific resource module.
     */
    public Builder withQueryResourceModule(final Module module)
    {
      queryResourceModule = module;
      return this;
    }

    public NativeQueryEngineModule build()
    {
      final Module queryRunnerFactoryModule = scanOnly
                                              ? new QueryRunnerFactoryModule(ImmutableSet.of(ScanQuery.class))
                                              : new QueryRunnerFactoryModule();
      final Class<? extends QuerySegmentWalker> walkerClass =
          querySegmentWalkerClass == null && scanOnly ? NoopQuerySegmentWalker.class : querySegmentWalkerClass;
      final Module querySegmentWalkerModule;
      if (walkerClass == null) {
        querySegmentWalkerModule = _ -> {};
      } else if (systemTableQueryEnabled) {
        querySegmentWalkerModule = binder -> {
          binder.bind(Key.get(QuerySegmentWalker.class, SystemTableQuerySegmentWalker.Delegate.class))
                .to(walkerClass)
                .in(LazySingleton.class);
          binder.bind(QuerySegmentWalker.class).to(SystemTableQuerySegmentWalker.class).in(LazySingleton.class);
        };
      } else {
        querySegmentWalkerModule = binder -> binder.bind(QuerySegmentWalker.class)
                                                   .to(walkerClass)
                                                   .in(LazySingleton.class);
      }

      final Module resolvedQueryResourceModule;
      if (queryResourceModule != null) {
        resolvedQueryResourceModule = queryResourceModule;
      } else if (!scanOnly) {
        resolvedQueryResourceModule = new QueryResourceModule();
      } else if (systemTableQueryEnabled) {
        resolvedQueryResourceModule = new SystemTableQueryResourceModule();
      } else {
        // Without the Druid engine for system tables, scan-only servers don't serve native queries.
        resolvedQueryResourceModule = _ -> {};
      }

      return new NativeQueryEngineModule(
          Modules.override(
              Modules.combine(
                  new QueryableModule(),
                  queryRunnerFactoryModule,
                  querySegmentWalkerModule
              )
          ).with(overrideModule),
          resolvedQueryResourceModule,
          systemTableQueryEnabled
      );
    }
  }

  @Override
  public void configure(final Binder binder)
  {
    binder.install(executionModule);
    binder.install(new SegmentWranglerModule());
    binder.install(new JoinableFactoryModule());
    if (systemTableQueryEnabled) {
      binder.install(new SystemTableModule());
    }
    binder.install(queryResourceModule);
  }

  @Override
  public List<com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return new QueryableModule().getJacksonModules();
  }
}
