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

package io.druid.query.aggregation.datasketches;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;

import com.yahoo.sketches.theta.Sketch;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.Arrays;
import java.util.List;

public class SketchModule implements DruidModule
{

  public static final String SET_SKETCH = "setSketch";
  public static final String SKETCH_BUILD = "sketchBuild";

  @Override
  public void configure(Binder binder)
  {
    //gives the extractor to ingest non-sketch input data
    if (ComplexMetrics.getSerdeForType(SKETCH_BUILD) == null) {
      ComplexMetrics.registerSerde(SKETCH_BUILD, new SketchBuildComplexMetricSerde());
    }

    //gives the extractor to ingest sketch input data
    if (ComplexMetrics.getSerdeForType(SET_SKETCH) == null) {
      ComplexMetrics.registerSerde(SET_SKETCH, new SketchMergeComplexMetricSerde());
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("SketchModule")
            .registerSubtypes(
                new NamedType(SketchBuildAggregatorFactory.class, "sketchBuild"),
                new NamedType(SketchMergeAggregatorFactory.class, "sketchMerge"),
                new NamedType(SketchEstimatePostAggregator.class, "sketchEstimate"),
                new NamedType(SketchSetPostAggregator.class, "sketchSetOper")
            )
            .addSerializer(Sketch.class, new SketchJsonSerializer())
    );
  }
}
