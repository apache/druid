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

package org.apache.druid.query.aggregation.exact.count.bitmap64;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.exact.count.bitmap64.sql.Bitmap64ExactCountSqlAggregator;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class Bitmap64ExactCountModule implements DruidModule
{
  public static final String TYPE_NAME = "Bitmap64ExactCount"; // common type name to be associated with segment data
  public static final String BUILD_TYPE_NAME = "Bitmap64ExactCountBuild";
  public static final String MERGE_TYPE_NAME = "Bitmap64ExactCountMerge";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("Bitmap64ExactCountModule")
            .registerSubtypes(
                new NamedType(Bitmap64ExactCountMergeAggregatorFactory.class, MERGE_TYPE_NAME),
                new NamedType(Bitmap64ExactCountBuildAggregatorFactory.class, BUILD_TYPE_NAME),
                new NamedType(Bitmap64ExactCountPostAggregator.class, "bitmap64ExactCount")
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
    SqlBindings.addAggregator(binder, Bitmap64ExactCountSqlAggregator.class);
  }

  @VisibleForTesting
  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(TYPE_NAME, new Bitmap64ExactCountMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(BUILD_TYPE_NAME, new Bitmap64ExactCountBuildComplexMetricSerde());
    ComplexMetrics.registerSerde(MERGE_TYPE_NAME, new Bitmap64ExactCountMergeComplexMetricSerde());
  }
}
