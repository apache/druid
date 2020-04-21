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
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.sql.avatica.AvaticaModule;
import org.apache.druid.sql.calcite.aggregation.SqlAggregationModule;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.planner.CalcitePlannerModule;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.schema.DruidCalciteSchemaModule;
import org.apache.druid.sql.calcite.view.DruidViewModule;
import org.apache.druid.sql.calcite.view.NoopViewManager;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.sql.http.SqlHttpModule;

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

      binder.bind(ViewManager.class).to(NoopViewManager.class).in(LazySingleton.class);

      binder.install(new DruidCalciteSchemaModule());
      binder.install(new CalcitePlannerModule());
      binder.install(new SqlAggregationModule());
      binder.install(new DruidViewModule());

      // QueryLookupOperatorConversion isn't in DruidOperatorTable since it needs a LookupExtractorFactoryContainerProvider injected.
      SqlBindings.addOperatorConversion(binder, QueryLookupOperatorConversion.class);

      if (isJsonOverHttpEnabled()) {
        binder.install(new SqlHttpModule());
      }

      if (isAvaticaEnabled()) {
        binder.install(new AvaticaModule());
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
