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

package io.druid.query.aggregation.datasketches.tuple.doublearray;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;

import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

/**
 * 
 * @author sunxin@rongcapital.cn
 *
 */
public class SketchModule implements DruidModule
{
  public static final String TUPLE_DOUBLE_SKETCH = "tupleDoubleArraySketch";
  public static final String TUPLE_DOUBLE_SKETCH_MERGE_AGG = "tupleDoubleArraySketchMerge";
  public static final String TUPLE_DOUBLE_SKETCH_BUILD_AGG = "tupleDoubleArraySketchBuild";


  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(TUPLE_DOUBLE_SKETCH) == null) {
      ComplexMetrics.registerSerde(TUPLE_DOUBLE_SKETCH, new SketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(TUPLE_DOUBLE_SKETCH_MERGE_AGG) == null) {
      ComplexMetrics.registerSerde(TUPLE_DOUBLE_SKETCH_MERGE_AGG, new SketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(TUPLE_DOUBLE_SKETCH_BUILD_AGG) == null) {
      ComplexMetrics.registerSerde(TUPLE_DOUBLE_SKETCH_BUILD_AGG, new SketchBuildComplexMetricSerde());
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("TupleDoubleArraySketchModule")
            .registerSubtypes(
                new NamedType(SketchMergeAggregatorFactory.class, TUPLE_DOUBLE_SKETCH)
            )
            .addSerializer(
                    ArrayOfDoublesSketch.class , new SketchJsonSerializer()
            )
    );
  }


}
