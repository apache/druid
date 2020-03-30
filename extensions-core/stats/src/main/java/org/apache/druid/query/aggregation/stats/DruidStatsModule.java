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

package org.apache.druid.query.aggregation.stats;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.teststats.PvaluefromZscorePostAggregator;
import org.apache.druid.query.aggregation.teststats.ZtestPostAggregator;
import org.apache.druid.query.aggregation.variance.StandardDeviationPostAggregator;
import org.apache.druid.query.aggregation.variance.VarianceAggregatorFactory;
import org.apache.druid.query.aggregation.variance.VarianceFoldingAggregatorFactory;
import org.apache.druid.query.aggregation.variance.VarianceSerde;
import org.apache.druid.query.aggregation.variance.sql.BaseVarianceSqlAggregator;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

/**
 */
public class DruidStatsModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule().registerSubtypes(
            VarianceAggregatorFactory.class,
            VarianceFoldingAggregatorFactory.class,
            StandardDeviationPostAggregator.class,
            ZtestPostAggregator.class,
            PvaluefromZscorePostAggregator.class
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (binder != null) {
      SqlBindings.addAggregator(binder, BaseVarianceSqlAggregator.VarPopSqlAggregator.class);
      SqlBindings.addAggregator(binder, BaseVarianceSqlAggregator.VarSampSqlAggregator.class);
      SqlBindings.addAggregator(binder, BaseVarianceSqlAggregator.VarianceSqlAggregator.class);
      SqlBindings.addAggregator(binder, BaseVarianceSqlAggregator.StdDevPopSqlAggregator.class);
      SqlBindings.addAggregator(binder, BaseVarianceSqlAggregator.StdDevSampSqlAggregator.class);
      SqlBindings.addAggregator(binder, BaseVarianceSqlAggregator.StdDevSqlAggregator.class);
    }
    ComplexMetrics.registerSerde("variance", new VarianceSerde());
  }
}
