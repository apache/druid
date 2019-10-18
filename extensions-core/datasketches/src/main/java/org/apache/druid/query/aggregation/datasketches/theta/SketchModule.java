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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchApproxCountDistinctSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchEstimateOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchEstimateWithErrorBoundsOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchObjectSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchSetIntersectOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchSetNotOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchSetUnionOperatorConversion;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class SketchModule implements DruidModule
{
  public static final String THETA_SKETCH = "thetaSketch";

  public static final String THETA_SKETCH_MERGE_AGG = "thetaSketchMerge";
  public static final String THETA_SKETCH_BUILD_AGG = "thetaSketchBuild";

  public static final String THETA_SKETCH_ESTIMATE_POST_AGG = "thetaSketchEstimate";
  public static final String THETA_SKETCH_SET_OP_POST_AGG = "thetaSketchSetOp";
  public static final String THETA_SKETCH_CONSTANT_POST_AGG = "thetaSketchConstant";
  public static final String THETA_SKETCH_TO_STRING_POST_AGG = "thetaSketchToString";

  @Override
  public void configure(Binder binder)
  {
    registerSerde();
    SqlBindings.addAggregator(binder, ThetaSketchApproxCountDistinctSqlAggregator.class);
    SqlBindings.addAggregator(binder, ThetaSketchObjectSqlAggregator.class);

    SqlBindings.addOperatorConversion(binder, ThetaSketchEstimateOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, ThetaSketchEstimateWithErrorBoundsOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, ThetaSketchSetIntersectOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, ThetaSketchSetUnionOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, ThetaSketchSetNotOperatorConversion.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.<Module>singletonList(
        new SimpleModule("ThetaSketchModule")
            .registerSubtypes(
                new NamedType(SketchMergeAggregatorFactory.class, THETA_SKETCH),
                new NamedType(SketchEstimatePostAggregator.class, THETA_SKETCH_ESTIMATE_POST_AGG),
                new NamedType(SketchSetPostAggregator.class, THETA_SKETCH_SET_OP_POST_AGG),
                new NamedType(SketchConstantPostAggregator.class, THETA_SKETCH_CONSTANT_POST_AGG),
                new NamedType(SketchToStringPostAggregator.class, THETA_SKETCH_TO_STRING_POST_AGG)
            )
            .addSerializer(SketchHolder.class, new SketchHolderJsonSerializer())
    );
  }

  @VisibleForTesting
  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(THETA_SKETCH, new SketchMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(THETA_SKETCH_MERGE_AGG, new SketchMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(THETA_SKETCH_BUILD_AGG, new SketchBuildComplexMetricSerde());
  }
}
