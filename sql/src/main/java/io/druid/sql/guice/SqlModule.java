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
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.server.initialization.jetty.JettyBindings;
import io.druid.server.metrics.MetricsModule;
import io.druid.sql.avatica.AvaticaMonitor;
import io.druid.sql.avatica.AvaticaServerConfig;
import io.druid.sql.avatica.DruidAvaticaHandler;
import io.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.CeilOperatorConversion;
import io.druid.sql.calcite.expression.ExtractOperatorConversion;
import io.druid.sql.calcite.expression.FloorOperatorConversion;
import io.druid.sql.calcite.expression.LookupOperatorConversion;
import io.druid.sql.calcite.expression.MillisToTimestampOperatorConversion;
import io.druid.sql.calcite.expression.RegexpExtractOperatorConversion;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.expression.SubstringOperatorConversion;
import io.druid.sql.calcite.expression.TimeArithmeticOperatorConversion;
import io.druid.sql.calcite.expression.TimeExtractOperatorConversion;
import io.druid.sql.calcite.expression.TimeFloorOperatorConversion;
import io.druid.sql.calcite.expression.TimeFormatOperatorConversion;
import io.druid.sql.calcite.expression.TimeParseOperatorConversion;
import io.druid.sql.calcite.expression.TimeShiftOperatorConversion;
import io.druid.sql.calcite.expression.TimestampToMillisOperatorConversion;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.view.NoopViewManager;
import io.druid.sql.calcite.view.ViewManager;
import io.druid.sql.http.SqlResource;

import java.util.List;
import java.util.Properties;

public class SqlModule implements Module
{
  public static final List<Class<? extends SqlAggregator>> DEFAULT_AGGREGATOR_CLASSES = ImmutableList.<Class<? extends SqlAggregator>>of(
      ApproxCountDistinctSqlAggregator.class
  );

  public static final List<Class<? extends SqlOperatorConversion>> DEFAULT_OPERATOR_CONVERSION_CLASSES = ImmutableList.<Class<? extends SqlOperatorConversion>>builder()
      .add(CeilOperatorConversion.class)
      .add(ExtractOperatorConversion.class)
      .add(FloorOperatorConversion.class)
      .add(LookupOperatorConversion.class)
      .add(MillisToTimestampOperatorConversion.class)
      .add(RegexpExtractOperatorConversion.class)
      .add(SubstringOperatorConversion.class)
      .add(TimeArithmeticOperatorConversion.TimeMinusIntervalOperatorConversion.class)
      .add(TimeArithmeticOperatorConversion.TimePlusIntervalOperatorConversion.class)
      .add(TimeExtractOperatorConversion.class)
      .add(TimeFloorOperatorConversion.class)
      .add(TimeFormatOperatorConversion.class)
      .add(TimeParseOperatorConversion.class)
      .add(TimeShiftOperatorConversion.class)
      .add(TimestampToMillisOperatorConversion.class)
      .build();

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

      for (Class<? extends SqlAggregator> clazz : DEFAULT_AGGREGATOR_CLASSES) {
        SqlBindings.addAggregator(binder, clazz);
      }

      for (Class<? extends SqlOperatorConversion> clazz : DEFAULT_OPERATOR_CONVERSION_CLASSES) {
        SqlBindings.addOperatorConversion(binder, clazz);
      }

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
