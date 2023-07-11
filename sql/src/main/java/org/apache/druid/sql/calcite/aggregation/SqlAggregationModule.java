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

package org.apache.druid.sql.calcite.aggregation;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.sql.calcite.aggregation.builtin.BuiltinApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import org.apache.druid.sql.guice.ApproxCountDistinct;
import org.apache.druid.sql.guice.SqlBindings;
import org.apache.druid.sql.guice.SqlModule;

/**
 * Module that provides SQL aggregations.
 *
 * To add an aggregation, use {@link SqlBindings#addAggregator}.
 *
 * To add an implementation option for APPROX_COUNT_DISTINCT, use {@link SqlBindings#addApproxCountDistinctChoice}.
 */
public class SqlAggregationModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    // Set up APPROX_COUNT_DISTINCT. As an additional bonus effect, this line ensures that the Guice binding
    // for SqlAggregator is set up.
    SqlBindings.addAggregator(binder, ApproxCountDistinctSqlAggregator.class);

    PolyBind.createChoiceWithDefault(
        binder,
        SqlModule.PROPERTY_SQL_APPROX_COUNT_DISTINCT_CHOICE,
        Key.get(SqlAggregator.class, ApproxCountDistinct.class),
        BuiltinApproxCountDistinctSqlAggregator.NAME
    );

    SqlBindings.addApproxCountDistinctChoice(
        binder,
        BuiltinApproxCountDistinctSqlAggregator.NAME,
        BuiltinApproxCountDistinctSqlAggregator.class
    );

    // Set up COUNT. Because it delegates to APPROX_COUNT_DISTINCT in certain cases, it must be added here
    // so it can have APPROX_COUNT_DISTINCT injected.
    SqlBindings.addAggregator(binder, CountSqlAggregator.class);
  }

  @Provides
  public ApproxCountDistinctSqlAggregator provideApproxCountDistinctSqlAggregator(
      @ApproxCountDistinct SqlAggregator aggregator
  )
  {
    return new ApproxCountDistinctSqlAggregator(aggregator);
  }
}
