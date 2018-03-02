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

package io.druid.query.aggregation.datasketches.tuple;

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
 * This module is to support numeric Tuple sketches, which extend the functionality of the count-distinct
 * Theta sketches by adding arrays of double values associated with unique keys.
 * 
 * https://datasketches.github.io/docs/Tuple/TupleOverview.html
 */
public class ArrayOfDoublesSketchModule implements DruidModule
{

  public static final String ARRAY_OF_DOUBLES_SKETCH = "arrayOfDoublesSketch";

  public static final String ARRAY_OF_DOUBLES_SKETCH_MERGE_AGG = "arrayOfDoublesSketchMerge";
  public static final String ARRAY_OF_DOUBLES_SKETCH_BUILD_AGG = "arrayOfDoublesSketchBuild";

  public static final String ARRAY_OF_DOUBLES_SKETCH_TO_ESTIMATE_POST_AGG = "arrayOfDoublesSketchToEstimate";
  public static final String ARRAY_OF_DOUBLES_SKETCH_TO_ESTIMATE_AND_BOUNDS_POST_AGG = "arrayOfDoublesSketchToEstimateAndBounds";
  public static final String ARRAY_OF_DOUBLES_SKETCH_TO_NUM_ENTRIES_POST_AGG = "arrayOfDoublesSketchToNumEntries";
  public static final String ARRAY_OF_DOUBLES_SKETCH_TO_MEANS_POST_AGG = "arrayOfDoublesSketchToMeans";
  public static final String ARRAY_OF_DOUBLES_SKETCH_TO_VARIANCES_POST_AGG = "arrayOfDoublesSketchToVariances";
  public static final String ARRAY_OF_DOUBLES_SKETCH_TO_QUANTILES_SKETCH_POST_AGG = "arrayOfDoublesSketchToQuantilesSketch";
  public static final String ARRAY_OF_DOUBLES_SKETCH_SET_OP_POST_AGG = "arrayOfDoublesSketchSetOp";
  public static final String ARRAY_OF_DOUBLES_SKETCH_T_TEST_POST_AGG = "arrayOfDoublesSketchTTest";
  public static final String ARRAY_OF_DOUBLES_SKETCH_TO_STRING_POST_AGG = "arrayOfDoublesSketchToString";

  @Override
  public void configure(final Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(ARRAY_OF_DOUBLES_SKETCH) == null) {
      ComplexMetrics.registerSerde(ARRAY_OF_DOUBLES_SKETCH, new ArrayOfDoublesSketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(ARRAY_OF_DOUBLES_SKETCH_MERGE_AGG) == null) {
      ComplexMetrics.registerSerde(
          ARRAY_OF_DOUBLES_SKETCH_MERGE_AGG,
          new ArrayOfDoublesSketchMergeComplexMetricSerde()
      );
    }

    if (ComplexMetrics.getSerdeForType(ARRAY_OF_DOUBLES_SKETCH_BUILD_AGG) == null) {
      ComplexMetrics.registerSerde(
          ARRAY_OF_DOUBLES_SKETCH_BUILD_AGG,
          new ArrayOfDoublesSketchBuildComplexMetricSerde()
      );
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("ArrayOfDoublesSketchModule").registerSubtypes(
            new NamedType(ArrayOfDoublesSketchAggregatorFactory.class, ARRAY_OF_DOUBLES_SKETCH),
            new NamedType(ArrayOfDoublesSketchToEstimatePostAggregator.class,
                ARRAY_OF_DOUBLES_SKETCH_TO_ESTIMATE_POST_AGG),
            new NamedType(ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator.class,
                ARRAY_OF_DOUBLES_SKETCH_TO_ESTIMATE_AND_BOUNDS_POST_AGG),
            new NamedType(ArrayOfDoublesSketchToNumEntriesPostAggregator.class,
                ARRAY_OF_DOUBLES_SKETCH_TO_NUM_ENTRIES_POST_AGG),
            new NamedType(ArrayOfDoublesSketchToMeansPostAggregator.class, ARRAY_OF_DOUBLES_SKETCH_TO_MEANS_POST_AGG),
            new NamedType(ArrayOfDoublesSketchToVariancesPostAggregator.class,
                ARRAY_OF_DOUBLES_SKETCH_TO_VARIANCES_POST_AGG),
            new NamedType(ArrayOfDoublesSketchToQuantilesSketchPostAggregator.class,
                ARRAY_OF_DOUBLES_SKETCH_TO_QUANTILES_SKETCH_POST_AGG),
            new NamedType(ArrayOfDoublesSketchSetOpPostAggregator.class, ARRAY_OF_DOUBLES_SKETCH_SET_OP_POST_AGG),
            new NamedType(ArrayOfDoublesSketchTTestPostAggregator.class, ARRAY_OF_DOUBLES_SKETCH_T_TEST_POST_AGG),
            new NamedType(ArrayOfDoublesSketchToStringPostAggregator.class, ARRAY_OF_DOUBLES_SKETCH_TO_STRING_POST_AGG))
            .addSerializer(ArrayOfDoublesSketch.class, new ArrayOfDoublesSketchJsonSerializer()
        )
    );
  }

}
