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

package org.apache.druid.query.aggregation.exact.cardinality.bitmap64;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.exact.cardinality.bitmap64.sql.Bitmap64ExactCardinalitySqlAggregator;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class Bitmap64ExactCardinalityModule implements DruidModule
{
  public static final String TYPE_NAME = "Bitmap64ExactCardinality"; // common type name to be associated with segment data
  public static final String BUILD_TYPE_NAME = "Bitmap64ExactCardinalityBuild";
  public static final String MERGE_TYPE_NAME = "Bitmap64ExactCardinalityMerge";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("Bitmap64ExactCardinalityModule")
            .registerSubtypes(
                new NamedType(Bitmap64ExactCardinalityMergeAggregatorFactory.class, MERGE_TYPE_NAME),
                new NamedType(Bitmap64ExactCardinalityBuildAggregatorFactory.class, BUILD_TYPE_NAME),
                new NamedType(Bitmap64ExactCardinalityPostAggregator.class, "bitmap64ExactCardinality")
            )
            .addSerializer(
                RoaringBitmap64Counter.class,
                new RoaringBitmap64CounterJsonSerializer()
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    registerSerde();
    SqlBindings.addAggregator(binder, Bitmap64ExactCardinalitySqlAggregator.class);
  }

  @VisibleForTesting
  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(TYPE_NAME, new Bitmap64ExactCardinalityMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(BUILD_TYPE_NAME, new Bitmap64ExactCardinalityBuildComplexMetricSerde());
    ComplexMetrics.registerSerde(MERGE_TYPE_NAME, new Bitmap64ExactCardinalityMergeComplexMetricSerde());
  }
}
