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

package org.apache.druid.sql.guice;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.server.initialization.jetty.JettyBindings;
import org.apache.druid.server.metrics.MetricsModule;
import org.apache.druid.sql.avatica.AvaticaMonitor;
import org.apache.druid.sql.avatica.AvaticaServerConfig;
import org.apache.druid.sql.avatica.DruidAvaticaHandler;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.view.NoopViewManager;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.sql.http.SqlResource;

import java.util.Properties;

public class SqlModule implements Module
{
  private static final String PROPERTY_SQL_ENABLE = "druid.sql.enable";
  private static final String PROPERTY_SQL_ENABLE_JSON_OVER_HTTP = "druid.sql.http.enable";
  private static final String PROPERTY_SQL_ENABLE_AVATICA = "druid.sql.avatica.enable";

  @Inject
  private Properties props;

  public SqlModule()
  {
  }

  @Override
  public void configure(Binder binder)
  {
    if (isEnabled()) {
      Calcites.setSystemProperties();

      JsonConfigProvider.bind(binder, "druid.sql.planner", PlannerConfig.class);
      JsonConfigProvider.bind(binder, "druid.sql.avatica", AvaticaServerConfig.class);
      LifecycleModule.register(binder, DruidSchema.class);
      binder.bind(ViewManager.class).to(NoopViewManager.class).in(LazySingleton.class);

      // Add empty SqlAggregator binder.
      Multibinder.newSetBinder(binder, SqlAggregator.class);

      // QueryLookupOperatorConversion isn't in DruidOperatorTable since it needs a LookupExtractorFactoryContainerProvider injected.
      SqlBindings.addOperatorConversion(binder, QueryLookupOperatorConversion.class);

      if (isJsonOverHttpEnabled()) {
        Jerseys.addResource(binder, SqlResource.class);
      }

      if (isAvaticaEnabled()) {
        binder.bind(AvaticaMonitor.class).in(LazySingleton.class);
        JettyBindings.addHandler(binder, DruidAvaticaHandler.class);
        MetricsModule.register(binder, AvaticaMonitor.class);
      }
    }
  }

  private boolean isEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE, "true"));
  }

  private boolean isJsonOverHttpEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true"));
  }

  private boolean isAvaticaEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE_AVATICA, "true"));
  }
}
