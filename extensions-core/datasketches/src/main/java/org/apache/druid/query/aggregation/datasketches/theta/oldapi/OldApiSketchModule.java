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

package org.apache.druid.query.aggregation.datasketches.theta.oldapi;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchBuildComplexMetricSerde;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolderJsonSerializer;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeComplexMetricSerde;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.segment.serde.ComplexMetrics;

import java.util.Collections;
import java.util.List;

public class OldApiSketchModule implements DruidModule
{

  public static final String SET_SKETCH = "setSketch";
  public static final String SKETCH_BUILD = "sketchBuild";
  public static final String SKETCH_MERGE = "sketchMerge";

  @Override
  public void configure(Binder binder)
  {
    ComplexMetrics.registerSerde(SKETCH_BUILD, new SketchBuildComplexMetricSerde());
    ComplexMetrics.registerSerde(SET_SKETCH, new SketchMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(SKETCH_MERGE, new SketchMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(SketchModule.THETA_SKETCH, new SketchMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(SketchModule.THETA_SKETCH_MERGE_AGG, new SketchMergeComplexMetricSerde());
    ComplexMetrics.registerSerde(SketchModule.THETA_SKETCH_BUILD_AGG, new SketchBuildComplexMetricSerde());
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.<Module>singletonList(
        new SimpleModule("OldThetaSketchModule")
            .registerSubtypes(
                new NamedType(OldSketchBuildAggregatorFactory.class, SKETCH_BUILD),
                new NamedType(OldSketchMergeAggregatorFactory.class, SKETCH_MERGE),
                new NamedType(OldSketchEstimatePostAggregator.class, "sketchEstimate"),
                new NamedType(OldSketchSetPostAggregator.class, "sketchSetOper")
            )
            .addSerializer(SketchHolder.class, new SketchHolderJsonSerializer())
    );
  }
}
