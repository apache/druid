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

package io.druid.sql.guice;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.server.initialization.jetty.JettyBindings;
import io.druid.server.metrics.MetricsModule;
import io.druid.sql.avatica.AvaticaMonitor;
import io.druid.sql.avatica.DruidAvaticaHandler;
import io.druid.sql.calcite.DruidSchema;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.http.SqlResource;
import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.SQLException;
import java.util.Properties;

public class SqlModule implements Module
{
  private static final String PROPERTY_SQL_ENABLE = "druid.sql.enable";
  private static final String PROPERTY_SQL_ENABLE_JSON_OVER_HTTP = "druid.sql.server.enableJsonOverHttp";
  private static final String PROPERTY_SQL_ENABLE_AVATICA = "druid.sql.server.enableAvatica";

  @Inject
  private Properties props;

  public SqlModule()
  {
  }

  @Override
  public void configure(Binder binder)
  {
    if (isEnabled()) {
      JsonConfigProvider.bind(binder, "druid.sql.planner", PlannerConfig.class);
      LifecycleModule.register(binder, DruidSchema.class);

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

  @Provides
  public CalciteConnection createCalciteConnection(
      final DruidSchema druidSchema,
      final PlannerConfig plannerConfig
  ) throws SQLException
  {
    if (isEnabled()) {
      return Calcites.jdbc(druidSchema, plannerConfig);
    } else {
      throw new IllegalStateException("Cannot provide CalciteConnection when SQL is disabled.");
    }
  }

  private boolean isEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE, "false"));
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
