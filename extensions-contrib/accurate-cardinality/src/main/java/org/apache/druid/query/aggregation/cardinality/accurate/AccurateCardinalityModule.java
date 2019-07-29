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

package org.apache.druid.query.aggregation.cardinality.accurate;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollectorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollectorComplexMetricSerde;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollectorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollectorJsonSerializer;
import org.apache.druid.query.aggregation.cardinality.accurate.sql.AccurateCardinalitySqlAggregator;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

public class AccurateCardinalityModule implements DruidModule
{
  public static final String ACCURATE_CARDINALITY = "accurateCardinality";
  public static final String BITMAP_COLLECTOR = "bitmapCollector";
  private static final String BITMPA_AGG = "bitmapAgg";

  private static final LongBitmapCollectorFactory DEFAULT_BITMAP_FACTORY = new LongRoaringBitmapCollectorFactory();

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("AccurateCardinalityModule")
            .registerSubtypes(
                new NamedType(AccurateCardinalityAggregatorFactory.class, ACCURATE_CARDINALITY),
                new NamedType(BitmapAggregatorFactory.class, BITMPA_AGG)
            )
            .addSerializer(
                LongRoaringBitmapCollector.class,
                new LongRoaringBitmapCollectorJsonSerializer()
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(BITMAP_COLLECTOR) == null) {
      ComplexMetrics.registerSerde(
          BITMAP_COLLECTOR,
          new LongRoaringBitmapCollectorComplexMetricSerde(DEFAULT_BITMAP_FACTORY)
      );
    }

    if (binder != null) {
      SqlBindings.addAggregator(binder, AccurateCardinalitySqlAggregator.class);
    }
  }
}
