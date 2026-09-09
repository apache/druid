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

package org.apache.druid.msq.querykit;

import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.msq.querykit.datasource.ExternalDataSourcePlanner;
import org.apache.druid.msq.querykit.datasource.FilteredDataSourcePlanner;
import org.apache.druid.msq.querykit.datasource.InlineDataSourcePlanner;
import org.apache.druid.msq.querykit.datasource.JoinDataSourcePlanner;
import org.apache.druid.msq.querykit.datasource.LookupDataSourcePlanner;
import org.apache.druid.msq.querykit.datasource.QueryDataSourcePlanner;
import org.apache.druid.msq.querykit.datasource.RestrictedDataSourcePlanner;
import org.apache.druid.msq.querykit.datasource.TableDataSourcePlanner;
import org.apache.druid.msq.querykit.datasource.UnionDataSourcePlanner;
import org.apache.druid.msq.querykit.datasource.UnnestDataSourcePlanner;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.sql.calcite.external.ExternalDataSource;

import java.util.Map;

/**
 * Provider of {@link DataSourcePlanner}.
 */
@LazySingleton
@SuppressWarnings("rawtypes")
public class DataSourcePlanners
{
  private static final TableDataSourcePlanner TABLE_PLANNER = new TableDataSourcePlanner();

  /**
   * Planners for builtin {@link DataSource} types.
   */
  private static final Map<Class<? extends DataSource>, DataSourcePlanner> BUILTIN =
      Map.ofEntries(
          Map.entry(TableDataSource.class, TABLE_PLANNER),
          Map.entry(GlobalTableDataSource.class, TABLE_PLANNER),
          Map.entry(RestrictedDataSource.class, new RestrictedDataSourcePlanner()),
          Map.entry(ExternalDataSource.class, new ExternalDataSourcePlanner()),
          Map.entry(InlineDataSource.class, new InlineDataSourcePlanner()),
          Map.entry(LookupDataSource.class, new LookupDataSourcePlanner()),
          Map.entry(FilteredDataSource.class, new FilteredDataSourcePlanner()),
          Map.entry(UnnestDataSource.class, new UnnestDataSourcePlanner()),
          Map.entry(QueryDataSource.class, new QueryDataSourcePlanner()),
          Map.entry(UnionDataSource.class, new UnionDataSourcePlanner()),
          Map.entry(JoinDataSource.class, new JoinDataSourcePlanner())
      );

  private final Map<Class<? extends DataSource>, DataSourcePlanner> planners;

  @Inject
  public DataSourcePlanners(Map<Class<? extends DataSource>, DataSourcePlanner> planners)
  {
    this.planners = planners;
  }

  @SuppressWarnings("unchecked")
  public <T extends DataSource> DataSourcePlanner<T> getPlanner(Class<T> dataSourceClass)
  {
    // Check extension planners first, so extensions can override builtin planners.
    final DataSourcePlanner extensionPlanner = planners.get(dataSourceClass);
    return extensionPlanner != null ? extensionPlanner : BUILTIN.get(dataSourceClass);
  }
}
