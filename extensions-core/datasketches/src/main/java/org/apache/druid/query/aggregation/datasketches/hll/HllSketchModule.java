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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.yahoo.sketches.hll.HllSketch;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchApproxCountDistinctSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchEstimateOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchEstimateWithErrorBoundsOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchObjectSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchSetUnionOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchToStringOperatorConversion;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

/**
 * This module is to support count-distinct operations using {@link HllSketch}.
 * See <a href="https://datasketches.github.io/docs/HLL/HLL.html">HyperLogLog Sketch documentation</a>
 */
public class HllSketchModule implements DruidModule
{

  public static final String TYPE_NAME = "HLLSketch"; // common type name to be associated with segment data
  public static final String BUILD_TYPE_NAME = "HLLSketchBuild";
  public static final String MERGE_TYPE_NAME = "HLLSketchMerge";
  public static final String TO_STRING_TYPE_NAME = "HLLSketchToString";
  public static final String UNION_TYPE_NAME = "HLLSketchUnion";
  public static final String ESTIMATE_WITH_BOUNDS_TYPE_NAME = "HLLSketchEstimateWithBounds";
  public static final String ESTIMATE_TYPE_NAME = "HLLSketchEstimate";


  @Override
  public void configure(final Binder binder)
  {
    registerSerde();
    SqlBindings.addAggregator(binder, HllSketchApproxCountDistinctSqlAggregator.class);
    SqlBindings.addAggregator(binder, HllSketchObjectSqlAggregator.class);

    SqlBindings.addOperatorConversion(binder, HllSketchEstimateOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, HllSketchEstimateWithErrorBoundsOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, HllSketchSetUnionOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, HllSketchToStringOperatorConversion.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("HllSketchModule").registerSubtypes(
            new NamedType(HllSketchMergeAggregatorFactory.class, MERGE_TYPE_NAME),
            new NamedType(HllSketchBuildAggregatorFactory.class, BUILD_TYPE_NAME),
            new NamedType(HllSketchMergeAggregatorFactory.class, TYPE_NAME),
            new NamedType(HllSketchToStringPostAggregator.class, TO_STRING_TYPE_NAME),
            new NamedType(HllSketchUnionPostAggregator.class, UNION_TYPE_NAME),
            new NamedType(HllSketchToEstimateWithBoundsPostAggregator.class, ESTIMATE_WITH_BOUNDS_TYPE_NAME),
            new NamedType(HllSketchToEstimatePostAggregator.class, ESTIMATE_TYPE_NAME)
        ).addSerializer(HllSketch.class, new HllSketchJsonSerializer())
    );
  }

  @VisibleForTesting
  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(TYPE_NAME, new HllSketchMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(BUILD_TYPE_NAME, new HllSketchBuildComplexMetricSerde());
    ComplexMetrics.registerSerde(MERGE_TYPE_NAME, new HllSketchMergeComplexMetricSerde());
  }
}
