/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.theta.oldapi;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.yahoo.sketches.theta.Sketch;
import io.druid.initialization.DruidModule;
import io.druid.query.aggregation.datasketches.theta.SketchBuildComplexMetricSerde;
import io.druid.query.aggregation.datasketches.theta.SketchJsonSerializer;
import io.druid.query.aggregation.datasketches.theta.SketchMergeComplexMetricSerde;
import io.druid.query.aggregation.datasketches.theta.SketchModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.Arrays;
import java.util.List;

public class OldApiSketchModule implements DruidModule
{

  public static final String SET_SKETCH = "setSketch";
  public static final String SKETCH_BUILD = "sketchBuild";
  public static final String SKETCH_MERGE = "sketchMerge";

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(SKETCH_BUILD) == null) {
      ComplexMetrics.registerSerde(SKETCH_BUILD, new SketchBuildComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(SET_SKETCH) == null) {
      ComplexMetrics.registerSerde(SET_SKETCH, new SketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(SKETCH_MERGE) == null) {
      ComplexMetrics.registerSerde(SKETCH_MERGE, new SketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(SketchModule.THETA_SKETCH) == null) {
      ComplexMetrics.registerSerde(SketchModule.THETA_SKETCH, new SketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(SketchModule.THETA_SKETCH_MERGE_AGG) == null) {
      ComplexMetrics.registerSerde(SketchModule.THETA_SKETCH_MERGE_AGG, new SketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(SketchModule.THETA_SKETCH_BUILD_AGG) == null) {
      ComplexMetrics.registerSerde(SketchModule.THETA_SKETCH_BUILD_AGG, new SketchBuildComplexMetricSerde());
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("OldThetaSketchModule")
            .registerSubtypes(
                new NamedType(OldSketchBuildAggregatorFactory.class, SKETCH_BUILD),
                new NamedType(OldSketchMergeAggregatorFactory.class, SKETCH_MERGE),
                new NamedType(OldSketchEstimatePostAggregator.class, "sketchEstimate"),
                new NamedType(OldSketchSetPostAggregator.class, "sketchSetOper")
            )
            .addSerializer(Sketch.class, new SketchJsonSerializer())
    );
  }
}
