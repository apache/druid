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
import com.google.inject.Provides;
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
import io.druid.sql.calcite.expression.CharacterLengthExtractionOperator;
import io.druid.sql.calcite.expression.ExtractExtractionOperator;
import io.druid.sql.calcite.expression.FloorExtractionOperator;
import io.druid.sql.calcite.expression.LookupExtractionOperator;
import io.druid.sql.calcite.expression.SqlExtractionOperator;
import io.druid.sql.calcite.expression.SubstringExtractionOperator;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.http.SqlResource;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Properties;

public class SqlModule implements Module
{
  public static final List<Class<? extends SqlAggregator>> DEFAULT_AGGREGATOR_CLASSES = ImmutableList.<Class<? extends SqlAggregator>>of(
      ApproxCountDistinctSqlAggregator.class
  );

  public static final List<Class<? extends SqlExtractionOperator>> DEFAULT_EXTRACTION_OPERATOR_CLASSES = ImmutableList.<Class<? extends SqlExtractionOperator>>of(
      CharacterLengthExtractionOperator.class,
      ExtractExtractionOperator.class,
      FloorExtractionOperator.class,
      LookupExtractionOperator.class,
      SubstringExtractionOperator.class
  );

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

      for (Class<? extends SqlAggregator> clazz : DEFAULT_AGGREGATOR_CLASSES) {
        SqlBindings.addAggregator(binder, clazz);
      }

      for (Class<? extends SqlExtractionOperator> clazz : DEFAULT_EXTRACTION_OPERATOR_CLASSES) {
        SqlBindings.addExtractionOperator(binder, clazz);
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

  @Provides
  public SchemaPlus createRootSchema(final DruidSchema druidSchema)
  {
    if (isEnabled()) {
      return Calcites.createRootSchema(druidSchema);
    } else {
      throw new IllegalStateException("Cannot provide SchemaPlus when SQL is disabled.");
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
