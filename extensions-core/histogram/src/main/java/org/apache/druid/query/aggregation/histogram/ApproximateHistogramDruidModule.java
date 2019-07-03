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

package org.apache.druid.query.aggregation.histogram;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.histogram.sql.FixedBucketsHistogramQuantileSqlAggregator;
import org.apache.druid.query.aggregation.histogram.sql.QuantileSqlAggregator;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

/**
 */
public class ApproximateHistogramDruidModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule().registerSubtypes(
            ApproximateHistogramFoldingAggregatorFactory.class,
            ApproximateHistogramAggregatorFactory.class,
            EqualBucketsPostAggregator.class,
            CustomBucketsPostAggregator.class,
            BucketsPostAggregator.class,
            QuantilesPostAggregator.class,
            QuantilePostAggregator.class,
            MinPostAggregator.class,
            MaxPostAggregator.class,
            FixedBucketsHistogramAggregatorFactory.class
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    registerSerde();
    SqlBindings.addAggregator(binder, QuantileSqlAggregator.class);
    SqlBindings.addAggregator(binder, FixedBucketsHistogramQuantileSqlAggregator.class);
  }

  @VisibleForTesting
  public static void registerSerde()
  {
    ComplexMetrics.registerSerde("approximateHistogram", new ApproximateHistogramFoldingSerde());
    ComplexMetrics.registerSerde(FixedBucketsHistogramAggregator.TYPE_NAME, new FixedBucketsHistogramSerde());
  }
}
